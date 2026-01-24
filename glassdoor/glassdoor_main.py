import datetime
import hashlib
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, quote

import psycopg2
import requests
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# =========================================================
# Glassdoor scraper (keyword + countries from JSON)
# ✅ Collect MANY links, open each detail page
# ✅ Correct salary extraction + normalization (detail page only)
# ✅ Auto-translate without API (Google unofficial endpoint)
# ✅ Saves to Postgres: job_id, job_url, title, company, location, skills, salary, date,
#          country (location_sub), keyword (title_sub)
# =========================================================

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))
COUNTRIES_PATH = Path(os.getenv("COUNTRIES_PATH", str(BASE_DIR / "countries.json")))
COOKIES_PATH = Path(os.getenv("COOKIES_PATH", str(BASE_DIR / "cookies.json")))

BASE_URL = "https://www.glassdoor.com"
SEARCH_URL = "https://www.glassdoor.com/Job"

DEFAULT_WAIT = int(os.getenv("DEFAULT_WAIT", "18"))
MAX_SCROLL = int(os.getenv("MAX_SCROLL", "80"))
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "1.1"))
NO_NEW_LIMIT = int(os.getenv("NO_NEW_LIMIT", "3"))
DETAIL_SLEEP = float(os.getenv("DETAIL_SLEEP", "0.35"))
BETWEEN_DETAIL_SLEEP = float(os.getenv("BETWEEN_DETAIL_SLEEP", "0.35"))

# Translation throttling (avoid ban)
TRANSLATE_MIN_INTERVAL = float(os.getenv("TRANSLATE_MIN_INTERVAL", "0.25"))

# Postgres env
DB_HOST = os.getenv("DB_HOST", "").strip()
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "").strip()
DB_USER = os.getenv("DB_USER", "").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()

_TABLE_READY = False

# -------------------------
# Shared HTTP session (translate)
# -------------------------
HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})

# =========================================================
# TRANSLATION (NO API) + CACHE
# =========================================================
_TRANSLATE_CACHE: dict[str, str] = {}
_last_translate_ts = 0.0


def contains_non_latin(text: str) -> bool:
    """
    Detect if string has CJK/Hangul/Kana etc (non-latin scripts).
    """
    if not text:
        return False
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af]", text))


def translate_to_en_no_api(text: str) -> str:
    """
    Google Translate unofficial endpoint (no API key)
    If fails -> returns original text
    """
    global _last_translate_ts

    t = (text or "").strip()
    if not t:
        return ""

    # only translate if looks non-latin
    if not contains_non_latin(t):
        return t

    if t in _TRANSLATE_CACHE:
        return _TRANSLATE_CACHE[t]

    # throttle
    now = time.time()
    wait = TRANSLATE_MIN_INTERVAL - (now - _last_translate_ts)
    if wait > 0:
        time.sleep(wait)

    try:
        q = quote(t)
        url = (
            "https://translate.googleapis.com/translate_a/single"
            f"?client=gtx&sl=auto&tl=en&dt=t&q={q}"
        )
        r = HTTP.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()
        out = "".join(x[0] for x in data[0] if x and x[0])
        out = (out or t).strip()

        _TRANSLATE_CACHE[t] = out
        _last_translate_ts = time.time()
        return out
    except Exception:
        _last_translate_ts = time.time()
        return t


def ensure_english(text: str) -> str:
    """
    Force translate if CJK/Hangul exists, otherwise return as-is.
    """
    t = (text or "").strip()
    if not t:
        return ""
    if contains_non_latin(t):
        return translate_to_en_no_api(t)
    return t


# =========================================================
# SALARY NORMALIZER
# =========================================================
def _fmt_int_spaces(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _detect_period(text: str) -> str:
    t = (text or "").lower()
    if "year" in t or "/yr" in t or "per year" in t or re.search(r"\byr\b", t):
        return "year"
    if "month" in t or "/mo" in t or "per month" in t or re.search(r"\bmo\b", t):
        return "month"
    if "hour" in t or "/hr" in t or "per hour" in t or re.search(r"\bhr\b", t):
        return "hour"
    return ""


def _detect_currency(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"[\$€£₽₸]", text)
    if m:
        return m.group(0)
    m = re.search(r"(?i)\b(usd|eur|gbp|rub|kzt|uah|cad|aud|chf)\b", text)
    if m:
        return m.group(1).upper()
    return ""


def _parse_money_token(tok: str) -> int | None:
    if not tok:
        return None
    s = tok.strip().replace(",", "").replace(" ", "")
    s = re.sub(r"^[\$€£₽₸]", "", s)

    m = re.match(r"^(\d+(?:\.\d+)?)(k|K)?$", s)
    if not m:
        return None
    num = float(m.group(1))
    if m.group(2):
        num *= 1000.0
    return int(round(num))


def normalize_glassdoor_salary(raw: str) -> str:
    """
    ✅ Fixes:
      - If no currency => return ""
      - Filters very small numbers (< 10)
      - Keeps / period if detected
    """
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""

    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\(.*?\)", "", s)  # remove "(Employer provided)" etc
    s = re.sub(r"\s+", " ", s).strip()

    cur = _detect_currency(s)
    period = _detect_period(s)

    if not cur:
        return ""

    s_clean = re.sub(
        r"(?i)(/year|per year|a year|/yr|year|/month|per month|a month|/mo|month|/hour|per hour|/hr|hour)",
        " ",
        s,
    )
    s_clean = re.sub(r"\s+", " ", s_clean).strip()

    tokens = re.findall(r"[\$€£₽₸]?\d[\d, ]*(?:\.\d+)?\s*[kK]?", s_clean)
    tokens = [t.strip() for t in tokens if t.strip()]

    nums = []
    for t in tokens:
        n = _parse_money_token(t)
        if n is not None:
            nums.append(n)

    nums = [n for n in nums if n >= 10]
    if not nums:
        return ""

    if len(nums) >= 2:
        out = f"{_fmt_int_spaces(nums[0])} - {_fmt_int_spaces(nums[1])}"
    else:
        out = f"{_fmt_int_spaces(nums[0])}"

    out = f"{out} {cur}".strip()
    if period:
        out = f"{out} / {period}".strip()
    return out


# =========================================================
# HASH
# =========================================================
def job_hash(title: str, company: str, location: str, date: datetime.date) -> str:
    raw = f"{title}|{company}|{location}|{date}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# =========================================================
# DB
# =========================================================
def ensure_table_exists(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.glassdoor (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT,
            job_url TEXT,
            job_hash CHAR(64) UNIQUE NOT NULL,
            title TEXT,
            company TEXT,
            location TEXT,
            location_sub TEXT,  -- country
            title_sub TEXT,     -- keyword
            skills TEXT,
            salary TEXT,
            date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute("ALTER TABLE public.glassdoor ADD COLUMN IF NOT EXISTS job_id TEXT;")
    cur.execute("ALTER TABLE public.glassdoor ADD COLUMN IF NOT EXISTS job_url TEXT;")
    cur.execute("ALTER TABLE public.glassdoor ADD COLUMN IF NOT EXISTS location_sub TEXT;")
    cur.execute("ALTER TABLE public.glassdoor ADD COLUMN IF NOT EXISTS title_sub TEXT;")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_glassdoor_job_id ON public.glassdoor(job_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_glassdoor_country ON public.glassdoor(location_sub);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_glassdoor_keyword ON public.glassdoor(title_sub);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_glassdoor_date ON public.glassdoor(date);")


class DB:
    def __init__(self):
        self.conn = None
        self.cur = None

    def open(self):
        global _TABLE_READY
        if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
            raise RuntimeError("DB env not set fully: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD (and optional DB_PORT)")

        self.conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        if not _TABLE_READY:
            ensure_table_exists(self.cur)
            self.conn.commit()
            _TABLE_READY = True

    def close(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self.conn = None
        self.cur = None

    def save(
        self,
        job_id: str,
        job_url: str,
        title: str,
        company: str,
        location: str,
        country: str,
        keyword: str,
        skills: str,
        salary: str,
        date: datetime.date,
    ):
        h = job_hash(title, company, location, date)
        self.cur.execute(
            """
            INSERT INTO public.glassdoor (
                job_id, job_url, job_hash,
                title, company, location,
                location_sub, title_sub,
                skills, salary, date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (job_hash) DO NOTHING
            """,
            (job_id, job_url, h, title, company, location, country, keyword, skills, salary, date),
        )
        self.conn.commit()
        if self.cur.rowcount == 0:
            print(f"⚠️ Duplicate skipped: {title} @ {company}")
        else:
            print(f"✅ Saved: {title} @ {company} | country={country} | kw={keyword} | salary={salary}")


# =========================================================
# SELENIUM
# =========================================================
def create_driver():
    options = uc.ChromeOptions()
    if os.getenv("HEADLESS", "false").strip().lower() in ("1", "true", "yes", "y", "on"):
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.page_load_strategy = "eager"
    return uc.Chrome(options=options)


def safe_quit(driver):
    try:
        if driver:
            driver.quit()
    except Exception:
        pass


def clear_and_type(el, text):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)


def safe_text(driver, by, sel) -> str:
    try:
        return (driver.find_element(by, sel).text or "").strip()
    except Exception:
        return ""


def close_popups(driver):
    xpaths = [
        "//button[@aria-label='Close']",
        "//button[contains(@class,'CloseButton')]",
        "//button[contains(@data-test,'close')]",
        "//div[contains(@class,'Modal') or contains(@class,'modal')]//button[contains(.,'Close')]",
    ]
    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for b in els[:2]:
                try:
                    b.click()
                    time.sleep(0.12)
                except Exception:
                    pass
        except Exception:
            pass


def load_cookies_if_any(driver):
    driver.get(BASE_URL)
    time.sleep(0.8)
    if COOKIES_PATH.exists():
        try:
            cookies = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
        except Exception:
            cookies = []
        for c in cookies:
            if isinstance(c, dict):
                c.pop("sameSite", None)
                try:
                    driver.add_cookie(c)
                except Exception:
                    pass
        driver.refresh()
        time.sleep(0.9)


# =========================================================
# DATE PARSER
# =========================================================
def parse_posted_date_from_text(t: str) -> datetime.date:
    today = datetime.date.today()
    s = (t or "").lower().strip()
    m = re.search(r"(\d+)\s*d\+?", s)  # "3d" or "30d+"
    if m:
        return today - datetime.timedelta(days=int(m.group(1)))
    if re.search(r"\b(\d+)\s*h\b", s) or "hour" in s:
        return today
    return today


# =========================================================
# SALARY EXTRACTION (DETAIL PAGE ONLY)
# =========================================================
def extract_salary_raw(driver) -> str:
    close_popups(driver)

    xpaths = [
        # most common
        "//*[@data-test='detailSalary']",
        "//*[@data-test='salaryEstimate']",
        "//*[@data-test='salary']",
        "//*[contains(@id,'job-salary')]",
        # class fallback
        "//*[contains(@class,'salaryEstimate') or contains(@class,'SalaryEstimate')]",
        # more generic but limit length
        "//*[contains(@data-test,'salary') and string-length(normalize-space(.)) < 260]",
    ]

    candidates = []
    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els[:12]:
                txt = (el.text or "").strip()
                if not txt:
                    continue
                # must contain currency
                if re.search(r"[\$€£₽₸]", txt) or re.search(r"(?i)\b(usd|eur|gbp|rub|kzt|uah|cad|aud|chf)\b", txt):
                    candidates.append(txt)
        except Exception:
            continue

    if not candidates:
        return ""

    def score(x: str) -> int:
        s = 0
        if re.search(r"\d", x): s += 2
        if "-" in x: s += 2
        if "/hr" in x.lower() or "hour" in x.lower(): s += 1
        if "/yr" in x.lower() or "year" in x.lower(): s += 1
        # prefer not too long
        s += max(0, 220 - len(x)) // 8
        return s

    candidates.sort(key=score, reverse=True)
    return candidates[0]


# =========================================================
# JSON LOADER
# =========================================================
def load_list_json(path: Path) -> list[str]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        for k in ("jobs", "keywords", "list", "countries"):
            if k in data and isinstance(data[k], list):
                data = data[k]
                break
    if not isinstance(data, list):
        return []
    out = []
    seen = set()
    for x in data:
        s = str(x).strip()
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


# =========================================================
# SCRAPER
# =========================================================
class GlassdoorScraper:
    def __init__(self, keyword: str, country: str, driver, db: DB):
        self.keyword = keyword
        self.country = country
        self.driver = driver
        self.db = db
        self.wait = WebDriverWait(driver, DEFAULT_WAIT)

    def open_search(self):
        self.driver.get(SEARCH_URL)
        time.sleep(0.9)
        close_popups(self.driver)

        job_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'jobTitle')]"))
        )
        loc_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'location')]"))
        )

        clear_and_type(job_input, f"\"{self.keyword}\"")
        clear_and_type(loc_input, self.country)
        loc_input.send_keys(Keys.ENTER)
        time.sleep(1.2)
        close_popups(self.driver)

        cur = self.driver.current_url
        if "sortBy=" not in cur:
            self.driver.get(cur + "&sortBy=date_desc")
        else:
            self.driver.get(cur.replace("sortBy=relevance", "sortBy=date_desc"))
        time.sleep(1.1)

    def collect_cards_links(self) -> list[dict]:
        results = []
        seen = set()
        no_new = 0
        last_len = 0

        for _ in range(MAX_SCROLL):
            close_popups(self.driver)
            cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")

            for li in cards:
                try:
                    a = li.find_element(By.XPATH, ".//a[contains(@href,'/partner/jobListing') or contains(@href,'/Job/')]")
                    href = a.get_attribute("href") or ""
                    if not href:
                        continue

                    url = href if href.startswith("http") else urljoin(BASE_URL, href)
                    if url in seen:
                        continue
                    seen.add(url)

                    posted = ""
                    loc = ""
                    try:
                        posted = (li.find_element(By.XPATH, ".//div[contains(@data-test,'job-age')]").text or "").strip()
                    except Exception:
                        pass
                    try:
                        loc = (li.find_element(By.XPATH, ".//div[contains(@data-test,'emp-location')]").text or "").strip()
                    except Exception:
                        pass

                    m = re.search(r"jobListingId=(\d+)", url)
                    job_id = m.group(1) if m else ""

                    results.append({"job_url": url, "job_id": job_id, "posted": posted, "location": loc})
                except Exception:
                    continue

            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)

            if len(results) == last_len:
                no_new += 1
            else:
                no_new = 0
                last_len = len(results)

            if no_new >= NO_NEW_LIMIT:
                break

        return results

    def parse_job_detail_page(self, url: str, fallback_location: str, posted_txt: str) -> dict:
        self.driver.get(url)
        close_popups(self.driver)

        self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//h1[contains(@id,'job-title') or @data-test='jobTitle']"))
        )
        time.sleep(DETAIL_SLEEP)
        close_popups(self.driver)

        title = safe_text(self.driver, By.XPATH, "//h1[contains(@id,'job-title') or @data-test='jobTitle']")
        company = safe_text(self.driver, By.XPATH, "//div[contains(@class,'EmployerProfile_employerNameHeading')]")

        location = safe_text(self.driver, By.XPATH, "//*[contains(@data-test,'location')][1]")
        if not location:
            location = fallback_location or ""

        raw_salary = extract_salary_raw(self.driver)
        salary = normalize_glassdoor_salary(raw_salary)

        skills = ""
        try:
            skills_elems = self.driver.find_elements(By.XPATH, "//li[contains(@class,'PendingQualification_pendingQualification')]")
            skills = ",".join(x.text.strip() for x in skills_elems if (x.text or "").strip())
        except Exception:
            skills = ""

        date = parse_posted_date_from_text(posted_txt)

        job_id = ""
        m = re.search(r"jobListingId=(\d+)", url)
        if m:
            job_id = m.group(1)

        # ✅ translate (no api)
        title = ensure_english(title)
        company = ensure_english(company)
        location = ensure_english(location)

        return {
            "job_id": job_id,
            "job_url": url,
            "title": title,
            "company": company,
            "location": location,
            "skills": skills,
            "salary": salary,
            "date": date,
        }

    def run(self):
        self.open_search()
        links = self.collect_cards_links()
        print(f"[COLLECT] keyword={self.keyword} country={self.country} collected_links={len(links)}")

        for item in links:
            try:
                detail = self.parse_job_detail_page(
                    item["job_url"],
                    fallback_location=item.get("location", ""),
                    posted_txt=item.get("posted", ""),
                )

                self.db.save(
                    job_id=detail["job_id"],
                    job_url=detail["job_url"],
                    title=detail["title"],
                    company=detail["company"],
                    location=detail["location"],
                    country=self.country,
                    keyword=self.keyword,
                    skills=detail["skills"],
                    salary=detail["salary"],
                    date=detail["date"],
                )
                time.sleep(BETWEEN_DETAIL_SLEEP)
            except Exception as e:
                print(f"[DETAIL FAIL] {item.get('job_url')} err={e}")
                time.sleep(0.6)


# =========================================================
# MAIN
# =========================================================
def main():
    jobs = load_list_json(JOBS_PATH)
    countries = load_list_json(COUNTRIES_PATH)

    print(f"[JOBS] {len(jobs)} -> {jobs[:10]}")
    print(f"[COUNTRIES] {len(countries)} -> {countries[:10]}")

    if not jobs:
        raise RuntimeError(f"job_list not found or empty: {JOBS_PATH}")
    if not countries:
        raise RuntimeError(f"countries not found or empty: {COUNTRIES_PATH}")

    driver = create_driver()
    db = DB()
    db.open()

    try:
        load_cookies_if_any(driver)

        for kw in jobs:
            for country in countries:
                try:
                    GlassdoorScraper(kw, country, driver=driver, db=db).run()
                except Exception as e:
                    print(f"[SEARCH FAIL] kw={kw} country={country} err={e}")

    finally:
        db.close()
        safe_quit(driver)


if __name__ == "__main__":
    main()
