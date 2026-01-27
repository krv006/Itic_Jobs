import datetime
import json
import os
import re
import time
from urllib.parse import urlparse, parse_qs, unquote_plus, quote_plus
from typing import Optional, Tuple, Set, Dict, List

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ✅ offline country resolver
import geonamescache
import pycountry

load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
TABLE_NAME = "public.hh"
DEFAULT_WAIT = 20

# ----------------------------
# FALLBACK ENGLISH NORMALIZER (NO API)
# ----------------------------
_RU2LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "yo", "ж": "j", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "x", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "shch", "ъ": "", "ы": "i", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}

_MAP_EXACT = {
    "ташкент": "Tashkent",
    "минск": "Minsk",
    "москва": "Moscow",
    "астана": "Astana",
    "алматы": "Almaty",
    "лимасол": "Limassol",
    "киев": "Kyiv",
    "санкт-петербург": "Saint Petersburg",

    "графический дизайнер": "Graphic Designer",
    "дизайнер": "Designer",
    "аниматор": "Animator",
    "анимация": "Animation",
    "моушн-дизайн": "Motion Design",
    "веб-дизайн": "Web Design",

    "за месяц": "per month",
    "на руки": "net",
    "от": "from",
    "до": "up to",
}

_MAP_IN_TEXT = {
    "за месяц": "per month",
    "на руки": "net",
    "обучение и развитие": "Training & Development",
    "аналитическое мышление": "Analytical Thinking",
    "управление командой": "Team Management",
    "планирование ресурсов": "Resource Planning",
    "разработка брендбука": "Brandbook Development",
    "разработка логотипов": "Logo Design",
    "визуализация данных": "Data Visualization",
    "стилизация изображений": "Image Styling",
    "моушн-дизайн": "Motion Design",
    "веб-дизайн": "Web Design",
}

_LANG_MAP = {
    "русский": "Russian",
    "английский": "English",
    "казахский": "Kazakh",
    "узбекский": "Uzbek",
}

_PROF_MAP = {
    "начальный": "Beginner",
    "средне-продвинутый": "Upper-Intermediate",
    "продвинутый": "Advanced",
    "свободно": "Fluent",
    "родной": "Native",
}

_TEXT_CACHE: dict[str, str] = {}

def _has_cyrillic(text: str) -> bool:
    return bool(text) and bool(re.search(r"[А-Яа-яЁё]", text))

def _translit_ru_to_lat(text: str) -> str:
    out = []
    for ch in text:
        low = ch.lower()
        if low in _RU2LAT:
            t = _RU2LAT[low]
            if ch.isupper() and t:
                t = t[0].upper() + t[1:]
            out.append(t)
        else:
            out.append(ch)
    return "".join(out)

def to_english(text: str) -> str:
    if not text:
        return ""

    s = text.strip()
    if not s:
        return ""

    if s in _TEXT_CACHE:
        return _TEXT_CACHE[s]

    low = s.lower()

    if low in _MAP_EXACT:
        res = _MAP_EXACT[low]
        _TEXT_CACHE[s] = res
        return res

    m = re.match(r"^\s*([А-Яа-яЁё]+)\s*[—-]\s*(A1|A2|B1|B2|C1|C2)\s*[—-]\s*([А-Яа-яЁё\- ]+)\s*$", s)
    if m:
        lang_ru = m.group(1).strip().lower()
        level = m.group(2).strip()
        prof_ru = m.group(3).strip().lower()
        lang_en = _LANG_MAP.get(lang_ru, _translit_ru_to_lat(m.group(1).strip()))
        prof_en = _PROF_MAP.get(prof_ru, _translit_ru_to_lat(m.group(3).strip()))
        res = f"{lang_en} - {level} - {prof_en}"
        _TEXT_CACHE[s] = res
        return res

    res = s
    for ru, en in _MAP_IN_TEXT.items():
        res = re.sub(re.escape(ru), en, res, flags=re.IGNORECASE)

    for ru, en in _LANG_MAP.items():
        res = re.sub(rf"\b{re.escape(ru)}\b", en, res, flags=re.IGNORECASE)
    for ru, en in _PROF_MAP.items():
        res = re.sub(rf"\b{re.escape(ru)}\b", en, res, flags=re.IGNORECASE)

    if _has_cyrillic(res):
        res = _translit_ru_to_lat(res)

    res = re.sub(r"\s+", " ", res).strip()
    _TEXT_CACHE[s] = res
    return res

def normalize_skills_csv(skills_csv: str) -> str:
    if not skills_csv:
        return ""
    items = [x.strip() for x in skills_csv.split(",") if x.strip()]
    out, seen = [], set()
    for it in items:
        en = to_english(it)
        if en and en not in seen:
            out.append(en)
            seen.add(en)
    return ",".join(out)

def normalize_salary_range(s: str) -> str:
    if not s:
        return ""
    raw = s.strip()
    if not raw:
        return ""

    t = raw.lower().replace("from", "ot").replace("up to", "do")

    cur = ""
    cur_m = re.search(r"(?i)\b(usd|eur|gbp|kzt|rub|uah|byn|br|pln|try|aed|sar|cad|aud|chf|sek|nok|dkk)\b", raw)
    if cur_m:
        cur = cur_m.group(1)
    else:
        sym_m = re.search(r"[\$€£₽₸]", raw)
        if sym_m:
            cur = sym_m.group(0)

    def _num_after(keyword: str) -> Optional[str]:
        m = re.search(rf"\b{keyword}\b\s*([\d\s]+)", t)
        if not m:
            return None
        n = re.sub(r"[^\d\s]", "", m.group(1))
        n = re.sub(r"\s+", " ", n).strip()
        return n or None

    frm = _num_after("ot")
    to = _num_after("do")

    if frm and to:
        out = f"{frm} - {to}"
    elif frm and not to:
        out = f"{frm} -"
    elif to and not frm:
        out = f"- {to}"
    else:
        nums = re.findall(r"\d[\d\s]*\d|\d+", t)
        nums = [re.sub(r"[^\d\s]", "", n) for n in nums]
        nums = [re.sub(r"\s+", " ", n).strip() for n in nums if n.strip()]
        if not nums:
            return ""
        out = f"{nums[0]} - {nums[1]}" if len(nums) >= 2 else nums[0]

    return f"{out} {cur}".strip() if cur else out


# ----------------------------
# COUNTRY RESOLVER (OFFLINE) for HH
# ----------------------------
gc = geonamescache.GeonamesCache()
_CITIES = gc.get_cities()
_COUNTRIES = gc.get_countries()

_CITY_TO_CC: Dict[str, Set[str]] = {}
for _id, c in _CITIES.items():
    name = (c.get("name") or "").strip().lower()
    cc = (c.get("countrycode") or "").strip().upper()
    if not name or not cc:
        continue
    _CITY_TO_CC.setdefault(name, set()).add(cc)

EXTRA_CITY_ALIASES = {
    "sf": "san francisco",
    "nyc": "new york",
    "la": "los angeles",
    "dc": "washington",
}
for k, v in EXTRA_CITY_ALIASES.items():
    if v in _CITY_TO_CC:
        _CITY_TO_CC.setdefault(k, set()).update(_CITY_TO_CC[v])

US_STATE_ABBR = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
    "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc"
}

CA_PROVINCES = {
    "ontario", "quebec", "british columbia", "alberta", "manitoba", "saskatchewan",
    "nova scotia", "new brunswick", "newfoundland and labrador", "prince edward island",
    "northwest territories", "nunavut", "yukon"
}

_LOCATION_CACHE: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

def _cc_to_country_name(cc: str) -> Optional[str]:
    cc = (cc or "").upper().strip()
    if not cc:
        return None
    info = _COUNTRIES.get(cc)
    if info and info.get("name"):
        return info["name"]
    obj = pycountry.countries.get(alpha_2=cc)
    return obj.name if obj else None

def _country_code_from_text(token: str) -> Optional[str]:
    t = (token or "").strip()
    if not t:
        return None
    low = t.lower()

    if low in {"us", "u.s.", "usa", "united states", "united states of america"}:
        return "US"
    if low in {"uk", "u.k.", "united kingdom", "britain", "england"}:
        return "GB"
    if low in {"uae", "united arab emirates"}:
        return "AE"

    # 2-letter code
    if re.fullmatch(r"[a-z]{2}", low):
        cc = low.upper()
        if pycountry.countries.get(alpha_2=cc):
            return cc
        return None

    # 3-letter -> alpha_2
    if re.fullmatch(r"[a-z]{3}", low):
        obj = pycountry.countries.get(alpha_3=low.upper())
        return obj.alpha_2 if obj else None

    # fuzzy
    try:
        matches = pycountry.countries.search_fuzzy(t)
        if matches:
            return matches[0].alpha_2
    except Exception:
        pass

    return None

def extract_country_name_and_code_from_location(location: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Input location example (after to_english):
      "Tashkent", "Almaty", "Tashkent, Uzbekistan", "Uzbekistan", "UZ", etc.
    Output:
      ("Uzbekistan", "UZ")
      ("Kazakhstan", "KZ")
      ("United States; Canada", "US; CA")
    """
    if not location:
        return None, None

    s = location.strip()
    if not s:
        return None, None

    key = s.lower()
    if key in _LOCATION_CACHE:
        return _LOCATION_CACHE[key]

    low = key

    # normalize separators and noise
    low = low.replace("&", " and ")
    low = re.sub(r"\bor\b", " ", low)
    low = re.sub(r"\s+", " ", low).strip()

    chunks = re.split(r"[;|/]", low)
    chunks = [c.strip() for c in chunks if c.strip()]

    found_cc: Set[str] = set()

    for chunk in chunks:
        parts = [p.strip() for p in chunk.split(",") if p.strip()]
        joined = " ".join(parts)

        # detect codes/names directly
        for p in parts:
            cc = _country_code_from_text(p)
            if cc:
                found_cc.add(cc)

        cc2 = _country_code_from_text(joined)
        if cc2:
            found_cc.add(cc2)

        # detect US/CA by state/province
        for p in parts:
            p2 = p.lower()
            if p2 in US_STATE_ABBR:
                found_cc.add("US")
            if p2 in CA_PROVINCES:
                found_cc.add("CA")

        # city->country via geonamescache (first part)
        if parts:
            city = parts[0].lower().strip()
            city = EXTRA_CITY_ALIASES.get(city, city)
            if city in _CITY_TO_CC:
                for cc in _CITY_TO_CC[city]:
                    found_cc.add(cc)

        # whole chunk as city
        c2 = chunk.lower().strip()
        c2 = EXTRA_CITY_ALIASES.get(c2, c2)
        if c2 in _CITY_TO_CC:
            for cc in _CITY_TO_CC[c2]:
                found_cc.add(cc)

    if not found_cc:
        res = (None, None)
        _LOCATION_CACHE[key] = res
        return res

    names: List[str] = []
    codes: List[str] = []
    for cc in sorted(found_cc):
        cname = _cc_to_country_name(cc)
        if cname and cname not in names:
            names.append(cname)
        if cc not in codes:
            codes.append(cc)

    res = ("; ".join(names), "; ".join(codes))
    _LOCATION_CACHE[key] = res
    return res


# ----------------------------
# DB
# ----------------------------
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)
conn.autocommit = True
cursor = conn.cursor()

def create_table_if_not_exists():
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            job_title TEXT,
            location TEXT,
            country TEXT,
            country_code TEXT,
            skills TEXT,
            salary TEXT,
            education TEXT,
            job_type TEXT,
            company_name TEXT,
            job_url TEXT,
            source TEXT,
            posted_date DATE,
            created_at TIMESTAMP DEFAULT NOW(),
            job_subtitle TEXT,
            search_query TEXT
        );
        """
    )

    # safety for existing table
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS id BIGSERIAL;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS job_subtitle TEXT;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS search_query TEXT;")

    # ✅ NEW
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS country TEXT;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS country_code TEXT;")

    cursor.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = split_part('{TABLE_NAME}', '.', 1)
                  AND tablename  = split_part('{TABLE_NAME}', '.', 2)
                  AND indexname  = 'hh_job_id_unique'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX hh_job_id_unique ON {TABLE_NAME} (job_id)';
            END IF;
        END$$;
        """
    )

def save_to_database(data: dict):
    cursor.execute(
        f"""
        INSERT INTO {TABLE_NAME} (
            job_id, job_title, location, country, country_code, skills, salary,
            education, job_type, company_name, job_url,
            source, posted_date, job_subtitle, search_query
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (job_id) DO UPDATE SET
            job_title    = EXCLUDED.job_title,
            location     = EXCLUDED.location,
            country      = COALESCE(EXCLUDED.country, {TABLE_NAME}.country),
            country_code = COALESCE(EXCLUDED.country_code, {TABLE_NAME}.country_code),
            skills       = EXCLUDED.skills,
            salary       = EXCLUDED.salary,
            education    = EXCLUDED.education,
            job_type     = EXCLUDED.job_type,
            company_name = EXCLUDED.company_name,
            job_url      = EXCLUDED.job_url,
            source       = EXCLUDED.source,
            posted_date  = EXCLUDED.posted_date,
            job_subtitle = EXCLUDED.job_subtitle,
            search_query = EXCLUDED.search_query;
        """,
        (
            data["job_id"],
            data["job_title"],
            data["location"],
            data.get("country"),
            data.get("country_code"),
            data["skills"],
            data["salary"],
            data["education"],
            data["job_type"],
            data["company_name"],
            data["job_url"],
            data["source"],
            data["posted_date"],
            data["job_subtitle"],
            data["search_query"],
        ),
    )


# ----------------------------
# DRIVER
# ----------------------------
def create_driver():
    options = uc.ChromeOptions()
    if os.getenv("HEADLESS", "false").lower() == "true":
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return uc.Chrome(options=options)

def safe_text(driver, xpath: str) -> str:
    try:
        return driver.find_element(By.XPATH, xpath).text.strip()
    except NoSuchElementException:
        return ""


# ----------------------------
# VALIDATION
# ----------------------------
def is_valid_job_id(job_id: str) -> bool:
    return bool(job_id) and job_id.isdigit() and len(job_id) >= 6

def is_valid_job_title(title: str) -> bool:
    if not title or len(title) < 5:
        return False
    bad_words = (
        "найдено", "vacancy", "employers", "работодател",
        "ооо ", "тоо ", "ип ", "ao ", "ltd",
    )
    t = title.lower()
    return not any(bad in t for bad in bad_words)


# ----------------------------
# SEARCH QUERY (from URL)
# ----------------------------
def extract_search_query_from_url(url: str) -> str:
    try:
        qs = parse_qs(urlparse(url).query)
        val = (qs.get("query") or qs.get("text") or [""])[0]
        return unquote_plus(val).strip()
    except Exception:
        return ""


# ----------------------------
# POSTED DATE (HH)
# ----------------------------
_RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def parse_posted_date_from_text(text: str) -> Optional[datetime.date]:
    if not text:
        return None
    t = text.strip().lower()
    today = datetime.date.today()

    if "сегодня" in t:
        return today
    if "вчера" in t:
        return today - datetime.timedelta(days=1)

    m = re.search(r"(\d+)\s*(дн(?:я|ей)|день)\s*назад", t)
    if m:
        return today - datetime.timedelta(days=int(m.group(1)))

    m = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", t)
    if m:
        day = int(m.group(1))
        mon_name = m.group(2)
        year = int(m.group(3))
        mon = _RU_MONTHS.get(mon_name)
        if mon:
            return datetime.date(year, mon, day)

    return None

def get_hh_posted_date(driver) -> datetime.date:
    candidates = []
    xpaths = [
        "//*[@data-qa='vacancy-view-creation-time']",
        "//*[contains(text(),'Вакансия опубликована')]",
        "//*[contains(text(),'Опубликовано')]",
    ]
    for xp in xpaths:
        txt = safe_text(driver, xp)
        if txt:
            candidates.append(txt)

    html = driver.page_source or ""
    m = re.search(r"(Вакансия опубликована[^<]{0,120})", html, flags=re.IGNORECASE)
    if m:
        candidates.append(m.group(1))
    m2 = re.search(r"(Опубликовано[^<]{0,120})", html, flags=re.IGNORECASE)
    if m2:
        candidates.append(m2.group(1))

    for c in candidates:
        dt = parse_posted_date_from_text(c)
        if dt:
            return dt

    return datetime.date.today()


# ----------------------------
# SEARCH PAGE URLS
# ----------------------------
def get_search_result_urls(driver, wait) -> List[str]:
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-qa='serp-item__title']")))
    time.sleep(0.2)

    links = driver.find_elements(By.CSS_SELECTOR, "a[data-qa='serp-item__title']")
    urls = []
    seen = set()

    for a in links:
        href = a.get_attribute("href")
        if not href:
            continue
        if "/vacancy/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)

    return urls


# ----------------------------
# SCRAPER
# ----------------------------
def get_hh_vacancies(jobs_list):
    create_table_if_not_exists()

    driver = create_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)

    try:
        for job in jobs_list:
            page = 0
            while True:
                q = quote_plus(str(job))
                search_url = f"https://tashkent.hh.uz/search/vacancy?text={q}&page={page}"
                driver.get(search_url)

                try:
                    urls = get_search_result_urls(driver, wait)
                except TimeoutException:
                    break

                if not urls:
                    break

                for url in urls:
                    time.sleep(0.45)
                    driver.get(url)

                    job_id = url.split("?")[0].split("/")[-1]
                    if not is_valid_job_id(job_id):
                        continue

                    raw_title = safe_text(driver, "//h1")
                    if not is_valid_job_title(raw_title):
                        continue

                    posted_date = get_hh_posted_date(driver)

                    raw_location = safe_text(driver, "//span[@data-qa='vacancy-view-raw-address']")
                    raw_skills = safe_text(driver, "//ul[contains(@class,'vacancy-skill-list')]").replace("\n", ",")
                    raw_salary = safe_text(driver, "//span[contains(@data-qa,'vacancy-salary')]")
                    raw_job_type = safe_text(driver, "//div[@data-qa='vacancy-working-hours']")
                    raw_company = safe_text(driver, "//div[@data-qa='vacancy-company__details']")

                    job_title = to_english(raw_title)

                    # normalize to english first
                    location = to_english(raw_location)
                    skills = normalize_skills_csv(raw_skills)
                    salary = normalize_salary_range(to_english(raw_salary))
                    job_type = to_english(raw_job_type)
                    company_name = to_english(raw_company)

                    # ✅ country + code from location
                    country, country_code = extract_country_name_and_code_from_location(location)

                    search_query = extract_search_query_from_url(url)

                    data = {
                        "job_id": job_id,
                        "job_title": job_title,
                        "location": location,
                        "country": country,
                        "country_code": country_code,
                        "skills": skills,
                        "salary": salary,
                        "education": "",
                        "job_type": job_type,
                        "company_name": company_name,
                        "job_url": url,
                        "source": "hh.uz",
                        "posted_date": posted_date,
                        "job_subtitle": str(job),
                        "search_query": search_query,
                    }

                    save_to_database(data)
                    print(f"SAVED: {job_id} | posted_date={posted_date} | country_code={country_code} | q={job}")

                page += 1

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def main():
    create_table_if_not_exists()

    with open("job_list.json", "r", encoding="utf-8") as f:
        jobs = json.load(f)

    if isinstance(jobs, dict):
        for k in ("jobs", "keywords", "list"):
            if k in jobs and isinstance(jobs[k], list):
                jobs = jobs[k]
                break

    jobs = [str(x).strip() for x in jobs if str(x).strip()]
    get_hh_vacancies(jobs)
    print("DONE")


if __name__ == "__main__":
    main()


"""
pip install geonamescache pycountry
"""
