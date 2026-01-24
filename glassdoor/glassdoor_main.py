import datetime
import hashlib
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
COOKIES_PATH = BASE_DIR / "cookies.json"
JOBS_PATH = BASE_DIR / "job_list.json"

BASE_URL = "https://www.glassdoor.com"
_TABLE_READY = False


# =========================
# SALARY NORMALIZER
# =========================
def _fmt_int_spaces(n: int) -> str:
    return f"{n:,}".replace(",", " ")

def _detect_period(text: str) -> str:
    t = (text or "").lower()
    # keep order: year > month > hour
    if "/year" in t or "per year" in t or " a year" in t or "/yr" in t or "year" in t:
        return "year"
    if "/month" in t or "per month" in t or " a month" in t or "/mo" in t or "month" in t:
        return "month"
    if "/hour" in t or "per hour" in t or "/hr" in t or "hour" in t:
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
    s = tok.strip()
    s = s.replace(",", "").replace(" ", "")
    s = re.sub(r"^[\$€£₽₸]", "", s)

    # 76,000 or 76K or 76.5K
    m = re.match(r"^(\d+(?:\.\d+)?)(k|K)?$", s)
    if not m:
        return None
    num = float(m.group(1))
    if m.group(2):
        num *= 1000.0
    return int(round(num))

def normalize_glassdoor_salary(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""

    # unify dash
    s = s.replace("–", "-").replace("—", "-")
    # remove bracket notes: (Employer provided) etc
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()

    cur = _detect_currency(s)
    period = _detect_period(s)

    # remove period words for numeric parse
    s_clean = re.sub(
        r"(?i)(/year|per year|a year|/yr|year|/month|per month|a month|/mo|month|/hour|per hour|/hr|hour)",
        " ",
        s,
    )
    s_clean = re.sub(r"\s+", " ", s_clean).strip()

    # capture money tokens: "$76,000", "84K", "$58", "72"
    tokens = re.findall(r"[\$€£₽₸]?\d[\d, ]*(?:\.\d+)?\s*[kK]?", s_clean)
    tokens = [t.strip() for t in tokens if t.strip()]

    nums = []
    for t in tokens:
        n = _parse_money_token(t)
        if n is not None:
            nums.append(n)

    if not nums:
        return ""

    if len(nums) >= 2:
        out = f"{_fmt_int_spaces(nums[0])} - {_fmt_int_spaces(nums[1])}"
    else:
        out = f"{_fmt_int_spaces(nums[0])}"

    if cur:
        out = f"{out} {cur}".strip()
    if period:
        out = f"{out} / {period}".strip()

    return out


# =========================
# HASH
# =========================
def job_hash(title, company, location, date):
    raw = f"{title}|{company}|{location}|{date}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# =========================
# DB
# =========================
def _env_required(key: str) -> str:
    v = os.getenv(key)
    return v.strip() if v else ""

def ensure_table_exists(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS glassdoor (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT UNIQUE,
            job_url TEXT,
            job_hash CHAR(64) UNIQUE NOT NULL,
            title TEXT,
            company TEXT,
            location TEXT,
            location_sub TEXT,
            title_sub TEXT,
            skills TEXT,
            salary TEXT,
            date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute("ALTER TABLE glassdoor ADD COLUMN IF NOT EXISTS job_id TEXT;")
    cur.execute("ALTER TABLE glassdoor ADD COLUMN IF NOT EXISTS job_url TEXT;")

class DB:
    def __init__(self):
        self.conn = None
        self.cur = None

    def open(self):
        global _TABLE_READY
        self.conn = psycopg2.connect(
            host=_env_required("DB_HOST"),
            port=int(_env_required("DB_PORT") or "5432"),
            dbname=_env_required("DB_NAME"),
            user=_env_required("DB_USER"),
            password=_env_required("DB_PASSWORD"),
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

    def save(self, job_id, job_url, title, company, location, location_sub, title_sub, skills, salary, date):
        h = job_hash(title, company, location, date)
        self.cur.execute(
            """
            INSERT INTO glassdoor (
                job_id, job_url, job_hash,
                title, company, location,
                location_sub, title_sub,
                skills, salary, date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (job_hash) DO NOTHING
            """,
            (job_id, job_url, h, title, company, location, location_sub, title_sub, skills, salary, date),
        )
        self.conn.commit()
        if self.cur.rowcount == 0:
            print(f"⚠️ Duplicate skipped: {title} @ {company}")
        else:
            print(f"✅ Saved: {title} @ {company} | salary={salary}")


# =========================
# DRIVER / HELPERS
# =========================
def create_driver():
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.page_load_strategy = "eager"
    return uc.Chrome(options=options)

def load_cookies_if_any(driver):
    driver.get(BASE_URL)
    time.sleep(1.0)
    if COOKIES_PATH.exists():
        with open(COOKIES_PATH, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        for c in cookies:
            c.pop("sameSite", None)
            try:
                driver.add_cookie(c)
            except Exception:
                pass
        driver.refresh()
        time.sleep(1.0)

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
    """
    Glassdoor popups are frequent. We try to close most common ones.
    Safe to call often.
    """
    xpaths = [
        "//button[@aria-label='Close']",
        "//button[contains(@class,'CloseButton')]",
        "//span[text()='Close']/ancestor::button[1]",
        "//button//*[name()='svg' and @data-test='icon-close']/ancestor::button[1]",
        "//div[contains(@class,'Modal') or contains(@class,'modal')]//button[contains(.,'Close')]",
        "//button[contains(.,'Continue')]",  # sometimes blocks content
        "//button[contains(.,'Not now')]",
    ]
    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                try:
                    els[0].click()
                    time.sleep(0.2)
                except Exception:
                    pass
        except Exception:
            pass

def parse_posted_date_from_text(t: str) -> datetime.date:
    today = datetime.date.today()
    s = (t or "").lower().strip()

    # "30d+"
    m = re.search(r"(\d+)\s*d\+", s)
    if m:
        return today - datetime.timedelta(days=int(m.group(1)))

    # "7d"
    m = re.search(r"(\d+)\s*d\b", s)
    if m:
        return today - datetime.timedelta(days=int(m.group(1)))

    # hours -> today
    if re.search(r"\b\d+\s*h\b", s) or "hour" in s:
        return today

    return today

def get_job_id_from_url(url: str) -> str:
    try:
        qs = parse_qs(urlparse(url).query)
        v = (qs.get("jobListingId") or [""])[0]
        return v.strip()
    except Exception:
        return ""

def wait_job_detail_loaded(driver, wait: WebDriverWait, job_id: str, timeout_sec: int = 12) -> bool:
    """
    Prevents stale panel issue: wait until page source contains this job_id.
    """
    if not job_id:
        return True

    end = time.time() + timeout_sec
    while time.time() < end:
        close_popups(driver)
        try:
            html = driver.page_source or ""
            if job_id in html:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False

def extract_salary_raw(driver) -> str:
    """
    Robust salary extraction:
    1) try multiple DOM selectors
    2) if still empty -> regex from page_source
    """
    close_popups(driver)

    selectors = [
        (By.XPATH, "//*[@data-test='detailSalary' or @data-test='salary' or @data-test='salaryEstimate']"),
        (By.XPATH, "//*[contains(@id,'job-salary')]"),
        (By.XPATH, "//*[contains(translate(., 'SALARY', 'salary'),'salary estimate')]"),
        (By.XPATH, "//*[contains(.,'/year') or contains(.,'per year') or contains(.,'/hr') or contains(.,'per hour') or contains(.,'/mo') or contains(.,'per month')]"),
    ]

    candidates = []
    for by, sel in selectors:
        try:
            els = driver.find_elements(by, sel)
            for el in els[:4]:
                txt = (el.text or "").strip()
                if txt and len(txt) <= 200:
                    candidates.append(txt)
        except Exception:
            continue

    # pick best candidate with money pattern
    money_re = re.compile(r"[\$€£₽₸]\s*\d", re.I)
    range_re = re.compile(r"[\$€£₽₸]?\s*\d[\d,\. ]*\s*[kK]?\s*[-–—]\s*[\$€£₽₸]?\s*\d", re.I)
    for c in candidates:
        if range_re.search(c) or money_re.search(c):
            return c

    # regex from source as fallback (very important for partner pages)
    try:
        html = driver.page_source or ""
        # examples: "$76,000 — $84,000/year" or "$127K - $194K"
        m = re.search(r"([\$€£₽₸]\s*\d[\d, ]*(?:\.\d+)?\s*[kK]?\s*[-–—]\s*[\$€£₽₸]\s*\d[\d, ]*(?:\.\d+)?\s*[kK]?(?:\s*/\s*(?:year|month|hour))?)", html, re.I)
        if m:
            return m.group(1)
        m2 = re.search(r"([\$€£₽₸]\s*\d[\d, ]*(?:\.\d+)?\s*[kK]?(?:\s*/\s*(?:year|month|hour))?)", html, re.I)
        if m2:
            return m2.group(1)
    except Exception:
        pass

    # last fallback: first candidate if any
    return candidates[0] if candidates else ""


# =========================
# SCRAPER
# =========================
class GlassdoorScraper:
    def __init__(self, job: str, country: str, driver, db: DB):
        self.job = job
        self.country = country
        self.driver = driver
        self.db = db
        self.wait = WebDriverWait(driver, 20)

    def open_search(self):
        self.driver.get("https://www.glassdoor.com/Job")
        time.sleep(1.0)
        close_popups(self.driver)

        job_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'jobTitle')]"))
        )
        loc_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'location')]"))
        )

        clear_and_type(job_input, f'"{self.job}"')
        clear_and_type(loc_input, self.country)
        loc_input.send_keys(Keys.ENTER)
        time.sleep(1.2)
        close_popups(self.driver)

        cur = self.driver.current_url
        if "sortBy=" not in cur:
            self.driver.get(cur + "&sortBy=date_desc")
        else:
            self.driver.get(cur.replace("sortBy=relevance", "sortBy=date_desc"))
        time.sleep(1.2)
        close_popups(self.driver)

    def collect_cards_links(self, max_scroll=90) -> list[dict]:
        results = []
        seen = set()
        no_new = 0
        last_len = 0

        for _ in range(max_scroll):
            close_popups(self.driver)

            cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
            for li in cards:
                try:
                    # strongest selectors for job link
                    a = None
                    for xp in [
                        ".//a[@data-test='job-link']",
                        ".//a[contains(@href,'/partner/jobListing') or contains(@href,'/Job/')]",
                        ".//a[contains(@href,'jobListingId=')]",
                    ]:
                        try:
                            a = li.find_element(By.XPATH, xp)
                            if a:
                                break
                        except Exception:
                            continue

                    if not a:
                        continue

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

                    job_id = get_job_id_from_url(url)

                    results.append({"job_url": url, "job_id": job_id, "posted": posted, "location": loc})

                except Exception:
                    continue

            # scroll
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)

            if len(results) == last_len:
                no_new += 1
            else:
                no_new = 0
                last_len = len(results)

            if no_new >= 4:
                break

        return results

    def parse_job_detail_page(self, url: str, job_id: str, fallback_location: str, posted_txt: str) -> dict:
        last_err = None
        for attempt in range(1, 3):
            try:
                self.driver.get(url)
                time.sleep(0.6)
                close_popups(self.driver)

                # wait title
                self.wait.until(
                    EC.presence_of_element_located((By.XPATH, "//h1[contains(@id,'job-title') or @data-test='jobTitle']"))
                )

                # crucial: wait correct vacancy is loaded (prevents salary carryover)
                ok = wait_job_detail_loaded(self.driver, self.wait, job_id, timeout_sec=12)
                if not ok and job_id:
                    # try refresh once
                    self.driver.refresh()
                    time.sleep(1.0)
                    close_popups(self.driver)
                    wait_job_detail_loaded(self.driver, self.wait, job_id, timeout_sec=10)

                title = safe_text(self.driver, By.XPATH, "//h1[contains(@id,'job-title') or @data-test='jobTitle']")
                company = safe_text(self.driver, By.XPATH, "//div[contains(@class,'EmployerProfile_employerNameHeading')]")

                # location from detail or fallback
                location = safe_text(self.driver, By.XPATH, "//*[contains(@data-test,'location')][1]")
                if not location:
                    location = fallback_location or ""

                # salary robust
                raw_salary = extract_salary_raw(self.driver)
                salary = normalize_glassdoor_salary(raw_salary)

                # skills
                skills = ""
                try:
                    skills_elems = self.driver.find_elements(
                        By.XPATH, "//li[contains(@class,'PendingQualification_pendingQualification')]"
                    )
                    skills = ",".join(x.text.strip() for x in skills_elems if (x.text or "").strip())
                except Exception:
                    skills = ""

                date = parse_posted_date_from_text(posted_txt)

                return {
                    "job_id": job_id or get_job_id_from_url(url),
                    "job_url": url,
                    "title": title,
                    "company": company,
                    "location": location,
                    "skills": skills,
                    "salary": salary,
                    "date": date,
                }

            except Exception as e:
                last_err = e
                try:
                    self.driver.refresh()
                except Exception:
                    pass
                time.sleep(1.0)

        raise last_err

    def run(self):
        self.open_search()
        links = self.collect_cards_links(max_scroll=90)
        print(f"[COLLECT] job={self.job} collected_links={len(links)}")

        for item in links:
            try:
                detail = self.parse_job_detail_page(
                    url=item["job_url"],
                    job_id=item.get("job_id", ""),
                    fallback_location=item.get("location", ""),
                    posted_txt=item.get("posted", ""),
                )

                self.db.save(
                    job_id=detail["job_id"],
                    job_url=detail["job_url"],
                    title=detail["title"],
                    company=detail["company"],
                    location=detail["location"],
                    location_sub=self.country,
                    title_sub=self.job,
                    skills=detail["skills"],
                    salary=detail["salary"],
                    date=detail["date"],
                )

                time.sleep(0.4)

            except Exception as e:
                print(f"[DETAIL FAIL] {item.get('job_url')} err={e}")
                time.sleep(0.8)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        jobs = json.load(f)
    if isinstance(jobs, dict):
        for k in ("jobs", "keywords", "list"):
            if k in jobs and isinstance(jobs[k], list):
                jobs = jobs[k]
                break
    jobs = [str(x).strip() for x in jobs if str(x).strip()]

    driver = create_driver()
    db = DB()
    db.open()

    try:
        load_cookies_if_any(driver)

        for job in jobs:
            try:
                GlassdoorScraper(job, "United States", driver=driver, db=db).run()
            except Exception as e:
                print(f"[JOB FAIL] {job} err={e}")

    finally:
        db.close()
        try:
            driver.quit()
        except Exception:
            pass
