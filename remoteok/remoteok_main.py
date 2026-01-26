import datetime
import json
import os
import re
import time
from pathlib import Path
from typing import List, Tuple, Set, Optional, Dict

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ✅ offline city->country
import geonamescache
import pycountry

# ================== CONFIG ==================
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

REMOTEOK_URL = "https://remoteok.com/"
SOURCE_NAME = "remoteok"

MAX_SCROLLS_PER_KEYWORD = 60   # har keyword uchun scroll limiti
SCROLL_PAUSE = 1.1
NO_NEW_LIMIT = 6
HEADLESS = False


# ================== SALARY NORMALIZER (REMOTEOK: $40k - $120k) ==================
def normalize_salary_k_range(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None

    s = raw.strip()
    if not s:
        return None

    # remove emoji + weird spaces
    s = re.sub(r"[💰\n\r\t]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # ignore premium text
    if "upgrade to premium" in s.lower():
        return None

    # detect currency
    cur = ""
    sym_m = re.search(r"[\$€£₽₸]", s)
    if sym_m:
        cur = sym_m.group(0)
    else:
        code_m = re.search(r"(?i)\b(usd|eur|gbp|kzt|rub|uah|byn|cad|aud|chf|sek|nok|dkk)\b", s)
        if code_m:
            cur = code_m.group(1).upper()

    # unify dash variants
    s2 = s.replace("–", "-").replace("—", "-")

    # find numbers with optional k/K
    nums = re.findall(r"(\d+(?:[.,]\d+)?)\s*([kK])?", s2)
    if not nums:
        return None

    def to_int_str(num_str: str, has_k: bool) -> str:
        num_str = num_str.replace(",", ".")
        val = float(num_str)
        if has_k:
            val *= 1000.0
        iv = int(round(val))
        return f"{iv:,}".replace(",", " ")

    values = [to_int_str(n, bool(k)) for n, k in nums]

    if len(values) >= 2:
        out = f"{values[0]} - {values[1]}"
    else:
        out = f"{values[0]}"

    if cur:
        return f"{out} {cur}".strip()
    return out


# ================== COUNTRY RESOLVER (OFFLINE, NAME + CODE) ==================
gc = geonamescache.GeonamesCache()
_CITIES = gc.get_cities()
_COUNTRIES = gc.get_countries()

# city_name_lower -> set(country_code)
_CITY_TO_CC: Dict[str, Set[str]] = {}
for _id, c in _CITIES.items():
    name = (c.get("name") or "").strip().lower()
    cc = (c.get("countrycode") or "").strip().upper()
    if not name or not cc:
        continue
    _CITY_TO_CC.setdefault(name, set()).add(cc)

# extra synonyms to improve hit rate
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

# memory cache to avoid repeating work
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

    # quick aliases
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

    # 3-letter code -> convert to alpha_2
    if re.fullmatch(r"[a-z]{3}", low):
        obj = pycountry.countries.get(alpha_3=low.upper())
        return obj.alpha_2 if obj else None

    # fuzzy by name
    try:
        matches = pycountry.countries.search_fuzzy(t)
        if matches:
            return matches[0].alpha_2
    except Exception:
        pass

    return None


def extract_country_name_and_code(location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns:
      country_name: "United States; Canada"
      country_code: "US; CA"
    """
    if not location:
        return None, None

    s = location.strip()
    if not s:
        return None, None

    cache_key = s.lower().strip()
    if cache_key in _LOCATION_CACHE:
        return _LOCATION_CACHE[cache_key]

    low = cache_key

    if "upgrade to premium" in low:
        _LOCATION_CACHE[cache_key] = (None, None)
        return None, None

    # worldwide/global
    if "🌏" in s or "worldwide" in low or "global" in low or "anywhere" in low:
        res = ("Worldwide", "WW")
        _LOCATION_CACHE[cache_key] = res
        return res

    # regions (optional)
    region_pairs: List[Tuple[str, str]] = []
    if "europe" in low:
        region_pairs.append(("Europe", "EU"))
    if "asia" in low:
        region_pairs.append(("Asia", "AS"))
    if "latam" in low or "latin america" in low:
        region_pairs.append(("Latin America", "LA"))

    # cleanup
    low = low.replace("&", " and ")
    low = re.sub(r"\bor\s+remote\b", " remote ", low)
    low = re.sub(r"\bor\b", " ", low)
    low = re.sub(r"\s+", " ", low).strip()

    chunks = re.split(r"[;|/]", low)
    chunks = [c.strip() for c in chunks if c.strip()]

    found_cc: Set[str] = set()
    found_remote = False

    for chunk in chunks:
        if "remote" in chunk:
            found_remote = True

        parts = [p.strip() for p in chunk.split(",") if p.strip()]
        joined = " ".join(parts)

        # direct country tokens
        for p in parts:
            cc = _country_code_from_text(p)
            if cc:
                found_cc.add(cc)

        cc2 = _country_code_from_text(joined)
        if cc2:
            found_cc.add(cc2)

        # US states / CA provinces
        for p in parts:
            p2 = p.lower()
            if p2 in US_STATE_ABBR:
                found_cc.add("US")
            if p2 in CA_PROVINCES:
                found_cc.add("CA")

        # city -> countrycode (geonamescache)
        if parts:
            city_candidate = parts[0].strip().lower()
            city_candidate = EXTRA_CITY_ALIASES.get(city_candidate, city_candidate)
            if city_candidate in _CITY_TO_CC:
                for cc in _CITY_TO_CC[city_candidate]:
                    found_cc.add(cc)

        c2 = chunk.strip().lower()
        c2 = EXTRA_CITY_ALIASES.get(c2, c2)
        if c2 in _CITY_TO_CC:
            for cc in _CITY_TO_CC[c2]:
                found_cc.add(cc)

    # build outputs
    name_out: List[str] = []
    code_out: List[str] = []

    for rn, rc in region_pairs:
        if rn not in name_out:
            name_out.append(rn)
        if rc not in code_out:
            code_out.append(rc)

    if found_remote and not found_cc and not region_pairs:
        name_out.append("Remote")
        code_out.append("REMOTE")

    for cc in sorted(found_cc):
        cname = _cc_to_country_name(cc)
        if cname and cname not in name_out:
            name_out.append(cname)
        if cc and cc not in code_out:
            code_out.append(cc)

    res = (
        "; ".join(name_out) if name_out else None,
        "; ".join(code_out) if code_out else None
    )
    _LOCATION_CACHE[cache_key] = res
    return res


# ================== DB ==================
def env_required(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f".env da {key} yo‘q yoki bo‘sh!")
    return v


def open_db():
    return psycopg2.connect(
        host=env_required("DB_HOST"),
        port=int(env_required("DB_PORT")),
        dbname=env_required("DB_NAME"),
        user=env_required("DB_USER"),
        password=env_required("DB_PASSWORD"),
    )


def ensure_table_exists(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS public.remoteok (
        id BIGSERIAL PRIMARY KEY,
        job_id TEXT NOT NULL UNIQUE,
        source TEXT NOT NULL,
        job_title TEXT,
        company_name TEXT,
        location TEXT,
        country TEXT,
        country_code TEXT,
        salary TEXT,
        job_type TEXT,
        skills TEXT,
        education TEXT,
        job_url TEXT,
        job_subtitle TEXT NULL,
        posted_at TIMESTAMP NULL,
        posted_date DATE NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        page INT,
        UNIQUE (job_id, source)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS posted_at TIMESTAMP NULL;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS posted_date DATE NULL;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS job_subtitle TEXT NULL;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS page INT;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS country TEXT;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS country_code TEXT;")
    conn.commit()


def insert_rows(conn, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO public.remoteok (
        job_id, source, job_title, company_name, location, country, country_code,
        salary, job_type, skills, education, job_url, page,
        posted_at, posted_date, job_subtitle
    )
    VALUES %s
    ON CONFLICT (job_id, source) DO UPDATE SET
        job_title     = EXCLUDED.job_title,
        company_name  = EXCLUDED.company_name,
        location      = EXCLUDED.location,
        country       = COALESCE(EXCLUDED.country, public.remoteok.country),
        country_code  = COALESCE(EXCLUDED.country_code, public.remoteok.country_code),
        salary        = EXCLUDED.salary,
        job_type      = EXCLUDED.job_type,
        skills        = EXCLUDED.skills,
        education     = EXCLUDED.education,
        job_url       = EXCLUDED.job_url,
        page          = EXCLUDED.page,
        posted_at     = COALESCE(EXCLUDED.posted_at, public.remoteok.posted_at),
        posted_date   = COALESCE(EXCLUDED.posted_date, public.remoteok.posted_date),
        job_subtitle  = COALESCE(EXCLUDED.job_subtitle, public.remoteok.job_subtitle);
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=250)
    conn.commit()
    return len(rows)


# ================== KEYWORDS ==================
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        raise RuntimeError(f"job_list.json topilmadi: {JOBS_PATH}")

    data = json.loads(JOBS_PATH.read_text(encoding="utf-8"))

    if isinstance(data, dict):
        for k in ("jobs", "keywords", "list"):
            if k in data and isinstance(data[k], list):
                data = data[k]
                break

    kws = []
    for x in data:
        s = str(x).strip()
        if s:
            kws.append(s)

    seen = set()
    out = []
    for k in kws:
        kl = k.lower()
        if kl not in seen:
            seen.add(kl)
            out.append(k)
    return out


def keyword_to_remoteok_url(keyword: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", keyword.strip().lower()).strip("-")
    return f"https://remoteok.com/remote-{slug}-jobs"


# ================== SELENIUM ==================
def create_driver():
    opts = webdriver.ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    else:
        opts.add_argument("--start-maximized")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )


def wait_ready(driver, timeout=30):
    WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


# ================== POSTED TIME PARSER ==================
def parse_relative_age(age_text: str) -> Optional[datetime.datetime]:
    if not age_text:
        return None

    t = age_text.strip().lower().replace(" ", "")
    now = datetime.datetime.now()

    m = re.match(r"^(\d+)(m|h|d|w|mo|y)$", t)
    if not m:
        return None

    n = int(m.group(1))
    unit = m.group(2)

    if unit == "m":
        return now - datetime.timedelta(minutes=n)
    if unit == "h":
        return now - datetime.timedelta(hours=n)
    if unit == "d":
        return now - datetime.timedelta(days=n)
    if unit == "w":
        return now - datetime.timedelta(weeks=n)
    if unit == "mo":
        return now - datetime.timedelta(days=30 * n)
    if unit == "y":
        return now - datetime.timedelta(days=365 * n)

    return None


def extract_posted_at_from_tr(tr) -> Optional[datetime.datetime]:
    selectors = ["td.time", ".time", "time"]

    for sel in selectors:
        try:
            el = tr.find_element(By.CSS_SELECTOR, sel)
            txt = safe_text(el)
            dt = parse_relative_age(txt)
            if dt:
                return dt
        except Exception:
            continue

    try:
        t_el = tr.find_element(By.CSS_SELECTOR, "time[datetime]")
        dt_raw = t_el.get_attribute("datetime")
        if dt_raw:
            try:
                dt_raw = dt_raw.replace("Z", "+00:00")
                return datetime.datetime.fromisoformat(dt_raw).replace(tzinfo=None)
            except Exception:
                pass
    except Exception:
        pass

    return None


# ================== ROW EXTRACTION ==================
def extract_rows(driver, page: int, job_subtitle: str) -> List[Tuple]:
    """
    Rows tuple (16 values):
      (job_id, source, title, company, loc, country, country_code, sal,
       job_type, skills, edu, link, page, posted_at, posted_date, job_subtitle)
    """
    rows = []
    trs = driver.find_elements(By.CSS_SELECTOR, "tr.job")
    for tr in trs:
        try:
            rid = tr.get_attribute("data-id")
            if not rid:
                continue

            job_id = f"remoteok_{rid}"

            title = None
            company = None

            try:
                title = safe_text(tr.find_element(By.CSS_SELECTOR, "h2")) or None
            except Exception:
                pass
            try:
                company = safe_text(tr.find_element(By.CSS_SELECTOR, "h3")) or None
            except Exception:
                pass

            loc = None
            loc_els = tr.find_elements(By.CSS_SELECTOR, ".location")
            if loc_els:
                loc = safe_text(loc_els[0]) or None

            # ✅ country + country_code
            country, country_code = extract_country_name_and_code(loc)

            sal = None
            sal_els = tr.find_elements(By.CSS_SELECTOR, ".salary")
            if sal_els:
                sal = safe_text(sal_els[0]) or None
            sal = normalize_salary_k_range(sal)

            tags = tr.find_elements(By.CSS_SELECTOR, ".tags a, .tags span")
            skills_list = [safe_text(t) for t in tags]
            skills_list = [x for x in skills_list if x]
            skills = ", ".join(skills_list) if skills_list else None

            job_type = None
            if skills:
                s = skills.lower()
                if "full" in s:
                    job_type = "Full-time"
                elif "part" in s:
                    job_type = "Part-time"
                elif "contract" in s:
                    job_type = "Contract"

            link = None
            a_els = tr.find_elements(By.CSS_SELECTOR, "a[href*='/remote-jobs/']")
            if a_els:
                link = a_els[0].get_attribute("href") or None

            posted_at = extract_posted_at_from_tr(tr)
            posted_date = posted_at.date() if posted_at else None

            rows.append(
                (
                    job_id,
                    SOURCE_NAME,
                    title,
                    company,
                    loc,
                    country,
                    country_code,
                    sal,
                    job_type,
                    skills,
                    None,   # education
                    link,
                    page,
                    posted_at,
                    posted_date,
                    job_subtitle,
                )
            )
        except Exception:
            continue

    return rows


# ================== SCROLL & INSERT PER KEYWORD ==================
def collect_for_keyword(driver, conn, keyword: str) -> None:
    seen: Set[str] = set()
    no_new = 0
    total_unique = 0
    total_upserted = 0

    for page in range(1, MAX_SCROLLS_PER_KEYWORD + 1):
        base_rows = extract_rows(driver, page, job_subtitle=keyword)

        fresh = []
        for r in base_rows:
            if r[0] not in seen:
                seen.add(r[0])
                fresh.append(r)

        new_count = len(fresh)
        total_unique += new_count
        print(f"[SCROLL] kw='{keyword}' page={page} total_unique={total_unique} new={new_count}")

        if new_count == 0:
            no_new += 1
        else:
            no_new = 0

        if fresh:
            up = insert_rows(conn, fresh)
            total_upserted += up
            print(f"[DB] kw='{keyword}' upserted={up} total_upserted={total_upserted}")

        if no_new >= NO_NEW_LIMIT:
            print(f"[STOP] kw='{keyword}' no_new_limit reached ({NO_NEW_LIMIT})")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

    print(f"[DONE] kw='{keyword}' total_unique={total_unique} total_upserted={total_upserted}")


def main():
    keywords = load_keywords()
    print(f"[KEYWORDS] {len(keywords)} -> {keywords}")

    conn = open_db()
    driver = None
    try:
        ensure_table_exists(conn)
        driver = create_driver()

        driver.get(REMOTEOK_URL)
        wait_ready(driver)

        for kw in keywords:
            url = keyword_to_remoteok_url(kw)
            print(f"\n[SEARCH] keyword='{kw}' -> {url}")

            driver.get(url)
            wait_ready(driver)

            collect_for_keyword(driver, conn, kw)

    finally:
        try:
            if driver:
                driver.quit()
        finally:
            conn.close()


if __name__ == "__main__":
    main()


"""
pip install geonamescache pycountry
"""
