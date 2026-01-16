import json
import os
import time
from pathlib import Path
from typing import List, Tuple, Set, Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from selenium import webdriver
from selenium.common.exceptions import (
    StaleElementReferenceException,
    ElementClickInterceptedException,
    NoSuchElementException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()

# ---------------- PATHS ----------------
BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

# ---------------- SETTINGS ----------------
REMOTEOK_URL = os.getenv("REMOTEOK_URL", "https://remoteok.com/")
SOURCE_NAME = os.getenv("REMOTEOK_SOURCE", "remoteok")

HEADLESS = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "30"))

MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "40"))
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "1.2"))
NO_NEW_LIMIT = int(os.getenv("NO_NEW_LIMIT", "4"))

SEARCH_RETRIES = int(os.getenv("SEARCH_RETRIES", "6"))
KEYWORD_DELAY = float(os.getenv("KEYWORD_DELAY", "1.2"))

# ✅ keyword natijasi umuman o‘zgarmasa reload qilib qayta urinadi
RESULT_CHANGE_RETRIES = int(os.getenv("RESULT_CHANGE_RETRIES", "3"))
AFTER_SEARCH_SETTLE = float(os.getenv("AFTER_SEARCH_SETTLE", "0.8"))


# ---------------- DB ----------------
def _env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return val


def open_db():
    conn = psycopg2.connect(
        host=_env_required("DB_HOST"),
        port=int(_env_required("DB_PORT")),
        dbname=_env_required("DB_NAME"),
        user=_env_required("DB_USER"),
        password=_env_required("DB_PASSWORD"),
    )
    conn.autocommit = False
    return conn


def ensure_table_exists(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.remoteok (
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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT ux_remoteok_jobid_source UNIQUE (job_id, source)
        );
        """
    )


def insert_rows(conn, rows: List[Tuple]) -> Tuple[int, int]:
    """
    returns (new_inserted, skipped_duplicates)
    """
    if not rows:
        return 0, 0

    sql = """
    INSERT INTO public.remoteok (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url
    )
    VALUES %s
    ON CONFLICT (job_id, source) DO NOTHING;
    """

    with conn.cursor() as cur:
        ensure_table_exists(cur)

        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        before = cur.fetchone()[0]

        execute_values(cur, sql, rows, page_size=200)

        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        after = cur.fetchone()[0]

    conn.commit()

    new_inserted = after - before
    skipped = max(len(rows) - new_inserted, 0)
    return new_inserted, skipped


# ---------------- INPUT ----------------
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        raise RuntimeError(f"job_list.json topilmadi: {JOBS_PATH}")
    data = json.loads(JOBS_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise RuntimeError('job_list.json LIST bo‘lishi kerak: ["python","react"]')

    # normalize + unique
    out, seen = [], set()
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


# ---------------- SELENIUM ----------------
def create_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def wait_ready(driver, timeout=25):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def find_search_input(driver):
    selectors = [
        (By.CSS_SELECTOR, "input#search"),
        (By.CSS_SELECTOR, "input[name='search']"),
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input[placeholder*='Search' i]"),
        (By.CSS_SELECTOR, "input[placeholder*='Type' i]"),
    ]
    for by, sel in selectors:
        els = driver.find_elements(by, sel)
        if els:
            return els[0]
    raise RuntimeError("Search input topilmadi.")


def get_first_visible_job_id(driver) -> Optional[str]:
    try:
        row = driver.find_element(By.CSS_SELECTOR, "tr.job")
        data_id = row.get_attribute("data-id") or ""
        return f"remoteok_{data_id}" if data_id else None
    except Exception:
        return None


def clear_and_type(driver, text: str):
    last_err = None
    for _ in range(SEARCH_RETRIES):
        try:
            el = find_search_input(driver)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.12)
            el.click()
            el.send_keys(Keys.CONTROL, "a")
            el.send_keys(Keys.DELETE)
            el.send_keys(text)
            el.send_keys(Keys.ENTER)
            time.sleep(KEYWORD_DELAY)
            return
        except (StaleElementReferenceException, ElementClickInterceptedException, NoSuchElementException) as e:
            last_err = e
            time.sleep(0.5)
    raise last_err if last_err else RuntimeError("Search write failed")


def apply_keyword(driver, kw: str) -> None:
    """
    Keyword qo‘ygandan keyin natija o‘zgarganini tekshiradi.
    O‘zgarmasa reload qilib qayta urinadi.
    """
    before = get_first_visible_job_id(driver)

    for attempt in range(1, RESULT_CHANGE_RETRIES + 1):
        clear_and_type(driver, kw)
        time.sleep(AFTER_SEARCH_SETTLE)
        after = get_first_visible_job_id(driver)

        # o‘zgardi yoki umuman natija yo‘q bo‘lsa OK
        if after is None or after != before:
            return

        # o‘zgarmadi -> reload
        driver.get(REMOTEOK_URL)
        wait_ready(driver, 25)
        time.sleep(0.8)

    # baribir o‘zgarmasa ham davom
    return


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def extract_job_rows(driver) -> List[Tuple]:
    out: List[Tuple] = []
    job_rows = driver.find_elements(By.CSS_SELECTOR, "tr.job")

    for row in job_rows:
        try:
            data_id = row.get_attribute("data-id") or ""
            job_id = f"remoteok_{data_id}" if data_id else None

            title_el = row.find_elements(By.CSS_SELECTOR, "h2")
            job_title = safe_text(title_el[0]) if title_el else None

            company_el = row.find_elements(By.CSS_SELECTOR, "h3")
            company_name = safe_text(company_el[0]) if company_el else None

            location = None
            loc_els = row.find_elements(By.CSS_SELECTOR, ".location")
            if loc_els:
                location = safe_text(loc_els[0]) or None

            salary = None
            sal_els = row.find_elements(By.CSS_SELECTOR, ".salary")
            if sal_els:
                salary = safe_text(sal_els[0]) or None

            tags = row.find_elements(By.CSS_SELECTOR, ".tags a, .tags span, a.tag")
            skills = ", ".join([safe_text(t) for t in tags if safe_text(t)]) or None

            job_type = None
            if skills:
                low = skills.lower()
                if "full-time" in low or "full time" in low:
                    job_type = "Full-time"
                elif "part-time" in low or "part time" in low:
                    job_type = "Part-time"
                elif "contract" in low:
                    job_type = "Contract"
                elif "freelance" in low:
                    job_type = "Freelance"
                elif "intern" in low:
                    job_type = "Internship"

            job_url = None
            a_els = row.find_elements(By.CSS_SELECTOR, "a[href*='/remote-jobs/']")
            if a_els:
                href = a_els[0].get_attribute("href")
                if href and href.startswith("http"):
                    job_url = href

            if not job_id:
                job_id = f"remoteok_{(job_title or 'job')}_{(company_name or 'company')}".replace(" ", "_")[:180]

            out.append(
                (job_id, SOURCE_NAME, job_title, company_name, location, salary, job_type, skills, None, job_url)
            )
        except Exception:
            continue

    return out


def scroll_collect(driver, global_seen_ids: Set[str]) -> List[Tuple]:
    """
    ✅ Global dedup: keywordlar orasida ham bir xil job qayta qayta yig‘ilmaydi
    """
    collected: List[Tuple] = []
    local_seen: Set[str] = set()

    no_new = 0

    for i in range(MAX_SCROLLS):
        rows = extract_job_rows(driver)

        new_count = 0
        for r in rows:
            jid = r[0]
            if jid in local_seen:
                continue
            local_seen.add(jid)

            if jid in global_seen_ids:
                continue
            global_seen_ids.add(jid)

            collected.append(r)
            new_count += 1

        if new_count == 0:
            no_new += 1
        else:
            no_new = 0

        print(f"[SCROLL] {i + 1}/{MAX_SCROLLS} total_unique={len(collected)} new={new_count}")

        if no_new >= NO_NEW_LIMIT:
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

    return collected


def main():
    keywords = load_keywords()
    print(f"[KEYWORDS] {len(keywords)} -> {keywords[:10]}{'...' if len(keywords) > 10 else ''}")

    conn = open_db()
    driver = None

    try:
        driver = create_driver()
        driver.get(REMOTEOK_URL)
        wait_ready(driver, 25)

        global_seen_ids: Set[str] = set()

        total_scraped = 0
        total_new = 0
        total_skipped = 0

        for kw in keywords:
            print(f"\n=== KEYWORD: {kw} ===")

            apply_keyword(driver, kw)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.4)

            rows = scroll_collect(driver, global_seen_ids)
            print(f"[COLLECT] keyword='{kw}' rows={len(rows)}")

            new_inserted, skipped = insert_rows(conn, rows)

            total_scraped += len(rows)
            total_new += new_inserted
            total_skipped += skipped

            print(f"[DB] keyword='{kw}' new_inserted={new_inserted} skipped={skipped}")

            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.8)

        print(
            f"\n[DONE] total_scraped_rows={total_scraped} total_new_inserted={total_new} total_skipped={total_skipped}")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print("[ERROR]", repr(e))
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            if driver:
                driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
