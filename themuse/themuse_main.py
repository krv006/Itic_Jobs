import datetime
import json
import os
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

# ================== PATH / CONFIG ==================
BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

SOURCE_NAME = "themuse"
BASE_URL = "https://www.themuse.com/search"

DEFAULT_WAIT = int(os.getenv("SELENIUM_WAIT", "25"))
HEADLESS = os.getenv("HEADLESS", "false").strip().lower() in ("1", "true", "yes")
MAX_PAGES = int(os.getenv("MAX_PAGES", "50"))
PAGE_SLEEP = float(os.getenv("PAGE_SLEEP", "0.6"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
MAX_STALE_RETRY = int(os.getenv("MAX_STALE_RETRY", "3"))


# ================== DB ==================
def open_db():
    """
    Supports:
    - DATABASE_URL
    - PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD
    - DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn

    host = os.getenv("PG_HOST") or os.getenv("DB_HOST")
    port = os.getenv("PG_PORT") or os.getenv("DB_PORT") or "5432"
    dbn = os.getenv("PG_DB") or os.getenv("DB_NAME")
    usr = os.getenv("PG_USER") or os.getenv("DB_USER")
    pwd = os.getenv("PG_PASSWORD") or os.getenv("DB_PASSWORD") or ""

    if not host or not dbn or not usr:
        raise RuntimeError(
            "DB env yo‘q: DATABASE_URL yoki PG_HOST/PG_DB/PG_USER yoki DB_HOST/DB_NAME/DB_USER to‘ldiring."
        )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbn,
        user=usr,
        password=pwd,
    )
    conn.autocommit = False
    return conn


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.themuse (
            id BIGSERIAL PRIMARY KEY,
            job_id VARCHAR(200) NOT NULL,
            source VARCHAR(50) NOT NULL,
            job_title VARCHAR(500),
            company_name VARCHAR(500),
            location VARCHAR(255),
            salary VARCHAR(255),
            job_type VARCHAR(255),
            skills TEXT,
            education VARCHAR(255),
            job_url TEXT,

            job_subtitle TEXT,
            posted_date DATE,

            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT ux_themuse_jobid_source UNIQUE (job_id, source)
        );
        """
    )
    cur.execute("ALTER TABLE public.themuse ADD COLUMN IF NOT EXISTS job_subtitle TEXT;")
    cur.execute("ALTER TABLE public.themuse ADD COLUMN IF NOT EXISTS posted_date DATE;")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_themuse_posted_date ON public.themuse (posted_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_themuse_job_subtitle ON public.themuse (job_subtitle);")


def upsert_jobs(cur, rows: List[Dict[str, Any]]):
    if not rows:
        return

    cols = [
        "job_id",
        "source",
        "job_title",
        "company_name",
        "location",
        "salary",
        "job_type",
        "skills",
        "education",
        "job_url",
        "job_subtitle",
        "posted_date",
    ]
    values = [tuple(r.get(c) for c in cols) for r in rows]

    sql = f"""
        INSERT INTO public.themuse ({",".join(cols)})
        VALUES %s
        ON CONFLICT (job_id, source) DO NOTHING;
    """
    execute_values(cur, sql, values, page_size=200)


def flush_to_db(conn, cur, batch_rows: List[Dict[str, Any]]):
    if not batch_rows:
        return
    upsert_jobs(cur, batch_rows)
    conn.commit()
    print(f"[DB] inserted_try={len(batch_rows)}")
    batch_rows.clear()


# ================== SELENIUM ==================
def create_driver():
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-gpu")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait(driver, timeout=DEFAULT_WAIT):
    return WebDriverWait(driver, timeout)


def safe_click(driver, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.08)
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False


def close_popups(driver):
    candidates = [
        (By.XPATH, "//button[contains(@aria-label,'Close') or contains(@aria-label,'close')]"),
        (By.XPATH, "//button[contains(.,'Close') or contains(.,'close')]"),
        (By.XPATH, "//button[contains(.,'Accept') or contains(.,'Got it') or contains(.,'I Accept')]"),
    ]
    for by, sel in candidates:
        try:
            for e in driver.find_elements(by, sel)[:2]:
                if e.is_displayed():
                    try:
                        driver.execute_script("arguments[0].click();", e)
                    except Exception:
                        pass
                    time.sleep(0.2)
        except Exception:
            pass


def close_extra_tabs(driver, main_handle: str):
    try:
        for h in driver.window_handles:
            if h != main_handle:
                try:
                    driver.switch_to.window(h)
                    driver.close()
                except Exception:
                    pass
        driver.switch_to.window(main_handle)
    except Exception:
        pass


# ================== URL / CARD HELPERS ==================
def build_search_url(keyword: str, page: int) -> str:
    kw = urllib.parse.quote(keyword.strip(), safe="")
    return f"{BASE_URL}/keyword/{kw}?page={page}"


def parse_job_id_from_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        job_param = qs.get("job", [None])[0]
        if job_param:
            return urllib.parse.unquote(job_param)
    except Exception:
        pass
    return url


def parse_location_from_card_text(card_text: str) -> Optional[str]:
    # Some cards show: "Title - Location Posted ..."
    if " - " in card_text:
        loc = card_text.split(" - ", 1)[1].strip()
        loc = re.sub(r"\s+Posted on.*$", "", loc, flags=re.IGNORECASE).strip()
        loc = re.sub(r"\s+Posted\s+\d+.*$", "", loc, flags=re.IGNORECASE).strip()
        return loc or None
    return None


def wait_left_list(driver):
    wait(driver).until(
        EC.presence_of_element_located(
            (By.XPATH, "//*[contains(translate(.,'view job','VIEW JOB'),'VIEW JOB')]")
        )
    )


def get_left_cards(driver) -> List[Tuple[Any, Any]]:
    view_els = driver.find_elements(
        By.XPATH,
        "//*[self::a or self::button][contains(translate(.,'view job','VIEW JOB'),'VIEW JOB')]"
    )

    cards = []
    for el in view_els:
        try:
            card = el.find_element(By.XPATH, "ancestor::div[1]/ancestor::div[1]")
            cards.append((card, el))
        except Exception:
            cards.append((None, el))
    return cards


def extract_right_text(driver) -> str:
    try:
        root = driver.find_element(By.XPATH, "//main")
        txt = (root.text or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    try:
        return (driver.find_element(By.TAG_NAME, "body").text or "").strip()
    except Exception:
        return ""


def extract_title(driver) -> Optional[str]:
    for sel in ("//main//h1", "//h1"):
        try:
            t = (driver.find_element(By.XPATH, sel).text or "").strip()
            if t and "jobs" not in t.lower():
                return t
        except Exception:
            pass
    return None


def company_from_text(detail_text: str) -> Optional[str]:
    m = re.search(r"\bAt\s+([A-Za-z0-9&.,'’\- ]{2,80})\b", detail_text)
    if m:
        name = m.group(1).strip()
        return name.split(" - ")[0].strip()
    return None


def extract_company(driver, detail_text: str) -> Optional[str]:
    selectors = [
        "//main//h1/preceding::*[self::a or self::span][1]",
        "//main//h1/preceding::a[1]",
        "//a[contains(@href,'/profiles/')][1]",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.XPATH, sel)
            tx = (el.text or "").strip()
            if tx and len(tx) < 120 and tx.lower() not in ("jobs", "companies", "advice", "coaching"):
                return tx
        except Exception:
            pass
    return company_from_text(detail_text)


def extract_salary(text: str) -> Optional[str]:
    m = re.search(r"(\$|£|€)\s?\d[\d,]*(\s?-\s?(\$|£|€)?\s?\d[\d,]*)?", text)
    return m.group(0).strip() if m else None


def detect_job_type(text: str) -> Optional[str]:
    t = text.lower()
    if "full-time" in t or "full time" in t:
        return "Full-time"
    if "part-time" in t or "part time" in t:
        return "Part-time"
    if "contract" in t:
        return "Contract"
    if "intern" in t or "internship" in t:
        return "Internship"
    return None


def detect_education(text: str) -> Optional[str]:
    t = text.lower()
    if "phd" in t or "doctorate" in t:
        return "PhD"
    if "master" in t or "msc" in t:
        return "Master"
    if "bachelor" in t or "undergraduate degree" in t:
        return "Bachelor"
    if "degree" in t:
        return "Degree required"
    return None


def extract_skills(text: str) -> Optional[str]:
    skills = [
        "python", "java", "javascript", "typescript", "react", "react native",
        "node", "django", "flask", "fastapi", "sql", "postgres", "mysql",
        "mongodb", "redis", "aws", "azure", "gcp", "docker", "kubernetes"
    ]
    t = " " + re.sub(r"\s+", " ", text.lower()) + " "
    found = sorted({s for s in skills if f" {s} " in t})
    return ", ".join(found) if found else None


def click_card_and_wait_detail(driver, el, main_handle: str):
    try:
        driver.execute_script("arguments[0].removeAttribute('target');", el)
    except Exception:
        pass

    old_title = ""
    try:
        old_title = (driver.find_element(By.XPATH, "//main//h1").text or "").strip()
    except Exception:
        pass

    safe_click(driver, el)
    time.sleep(0.25)
    close_extra_tabs(driver, main_handle)

    def title_changed(d):
        try:
            t = (d.find_element(By.XPATH, "//main//h1").text or "").strip()
            return t and t != old_title and "jobs" not in t.lower()
        except Exception:
            return False

    try:
        wait(driver).until(title_changed)
    except TimeoutException:
        wait(driver).until(EC.presence_of_element_located((By.XPATH, "//main//h1")))


# ================== ✅ POSTED DATE PARSER ==================
_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_posted_date(text: str) -> Optional[datetime.date]:
    """
    Examples:
    - "7h", "Posted 7h ago", "7 hours ago"
    - "2d", "2 days ago"
    - "3w", "3 weeks ago"
    - "1mo", "1 month ago"
    - "Posted on January 23, 2026"
    - "Posted on Jan 23, 2026"
    - "01/23/2026"
    """
    if not text:
        return None

    t = " ".join(text.split()).strip().lower()
    if not t:
        return None

    today = datetime.date.today()

    # absolute month name: posted on jan 23, 2026
    m = re.search(r"posted on\s+([a-z]+)\s+(\d{1,2}),\s*(\d{4})", t, flags=re.I)
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            try:
                return datetime.date(int(m.group(3)), mon, int(m.group(2)))
            except Exception:
                pass

    # numeric date
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        y = int(m.group(3))
        # assume US mm/dd/yyyy first
        try:
            return datetime.date(y, a, b)
        except Exception:
            try:
                return datetime.date(y, b, a)
            except Exception:
                pass

    # relative hours
    m = re.search(r"\b(\d+)\s*(h|hr|hrs|hour|hours)\b", t)
    if m:
        hours = int(m.group(1))
        return today - datetime.timedelta(days=(hours // 24))

    # relative days
    m = re.search(r"\b(\d+)\s*(d|day|days)\b", t)
    if m:
        return today - datetime.timedelta(days=int(m.group(1)))

    # relative weeks
    m = re.search(r"\b(\d+)\s*(w|week|weeks)\b", t)
    if m:
        return today - datetime.timedelta(days=int(m.group(1)) * 7)

    # relative months (~30d)
    m = re.search(r"\b(\d+)\s*(mo|mos|month|months)\b", t)
    if m:
        return today - datetime.timedelta(days=int(m.group(1)) * 30)

    if "today" in t:
        return today
    if "yesterday" in t:
        return today - datetime.timedelta(days=1)

    return None


def extract_posted_date(card_text: str, detail_text: str) -> datetime.date:
    for txt in (card_text or "", detail_text or ""):
        d = parse_posted_date(txt)
        if d:
            return d
    return datetime.date.today()


# ================== KEYWORDS ==================
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        raise RuntimeError(f"job_list.json topilmadi: {JOBS_PATH}")

    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return [str(x).strip() for x in data if str(x).strip()]

    for key in ("keywords", "jobs", "job_titles"):
        if key in data and isinstance(data[key], list):
            return [str(x).strip() for x in data[key] if str(x).strip()]

    raise RuntimeError("job_list.json format topilmadi. List yoki {keywords:[...]} bo‘lsin.")


# ================== SCRAPER ==================
def scrape_keyword(driver, keyword: str, conn):
    cur = conn.cursor()
    ensure_table(cur)
    conn.commit()

    print(f"\n=== KEYWORD: {keyword} ===")
    main_handle = driver.current_window_handle

    for page in range(1, MAX_PAGES + 1):
        url = build_search_url(keyword, page)
        print(f"[OPEN] {url}")

        driver.get(url)
        time.sleep(PAGE_SLEEP)
        close_popups(driver)

        try:
            wait_left_list(driver)
        except TimeoutException:
            print(f"[STOP] No results/blocked keyword='{keyword}' page={page}")
            break

        batch_rows: List[Dict[str, Any]] = []
        seen_ids = set()

        i = 0
        while True:
            close_popups(driver)
            close_extra_tabs(driver, main_handle)

            cards = get_left_cards(driver)
            if i >= len(cards):
                break

            stale_retry = 0
            while stale_retry < MAX_STALE_RETRY:
                try:
                    card, view_el = cards[i]
                    card_text = (card.text or "").strip() if card is not None else ""
                    location = parse_location_from_card_text(card_text)

                    click_card_and_wait_detail(driver, view_el, main_handle)
                    time.sleep(0.2)
                    close_popups(driver)
                    close_extra_tabs(driver, main_handle)

                    job_url = driver.current_url
                    job_id = parse_job_id_from_url(job_url)

                    if not job_id or job_id in seen_ids:
                        break
                    seen_ids.add(job_id)

                    detail_text = extract_right_text(driver)

                    title = extract_title(driver)
                    company = extract_company(driver, detail_text)
                    posted_date = extract_posted_date(card_text, detail_text)

                    row = {
                        "job_id": job_id,
                        "source": SOURCE_NAME,
                        "job_title": title,
                        "company_name": company,
                        "location": location,
                        "salary": extract_salary(detail_text),
                        "job_type": detect_job_type(detail_text),
                        "skills": extract_skills(detail_text),
                        "education": detect_education(detail_text),
                        "job_url": job_url,

                        # ✅ REQUIRED
                        "job_subtitle": keyword,
                        "posted_date": posted_date,
                    }

                    batch_rows.append(row)
                    print(f"  [JOB] {title} | {company} | {location} | posted_date={posted_date} | id={job_id}")

                    if len(batch_rows) >= BATCH_SIZE:
                        flush_to_db(conn, cur, batch_rows)

                    time.sleep(PAGE_SLEEP)
                    break

                except StaleElementReferenceException:
                    stale_retry += 1
                    time.sleep(0.2)
                    continue
                except Exception as e:
                    print("  [ERR]", repr(e))
                    break

            i += 1

        try:
            flush_to_db(conn, cur, batch_rows)
        except Exception as e:
            conn.rollback()
            print("[DB ERR]", repr(e))
            batch_rows.clear()

        print(f"[PAGE DONE] page={page} unique_jobs={len(seen_ids)}")
        if len(seen_ids) == 0:
            print(f"[STOP] empty list keyword='{keyword}' page={page}")
            break


def main():
    keywords = load_keywords()
    conn = open_db()
    print("[DB] connected:", conn.get_dsn_parameters())

    driver = create_driver()
    try:
        for kw in keywords:
            kw = kw.strip()
            if kw:
                scrape_keyword(driver, kw, conn)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
