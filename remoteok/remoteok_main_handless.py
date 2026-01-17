import json
import os
import re
import time
from pathlib import Path
from typing import List, Tuple, Set, Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ================== LOAD ENV ==================
load_dotenv()

# ================== PATHS / CONFIG ==================
BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

REMOTEOK_URL = "https://remoteok.com/"
SOURCE_NAME = "remoteok"

MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "60"))
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "0.45"))
NO_NEW_LIMIT = int(os.getenv("NO_NEW_LIMIT", "3"))


# ================== ENV HELPERS ==================
def env_required(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f".env da {key} yo‘q yoki bo‘sh!")
    return v


def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


# ================== BROWSER SETTINGS (.env) ==================
HEADLESS = env_bool("HEADLESS", False)
PAGE_LOAD_TIMEOUT = env_int("PAGE_LOAD_TIMEOUT", 20)
IMPLICIT_WAIT = env_int("IMPLICIT_WAIT", 5)
CHROME_WINDOW_SIZE = os.getenv("CHROME_WINDOW_SIZE", "1920,1080")


# ================== DB ==================
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
        job_id TEXT NOT NULL,
        source TEXT NOT NULL,
        job_title TEXT,
        company_name TEXT,
        location TEXT,
        salary TEXT,
        job_type TEXT,
        skills TEXT,
        education TEXT,
        job_url TEXT,
        page INT,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE (job_id, source)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def insert_rows(conn, rows: List[Tuple]) -> Tuple[int, int]:
    """
    FAST insert:
    - COUNT(*) yo'q
    - commit faqat insert bo'lsa
    returns: (inserted_estimate, skipped_estimate)
    """
    if not rows:
        return 0, 0

    sql = """
    INSERT INTO public.remoteok (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url, page
    )
    VALUES %s
    ON CONFLICT (job_id, source) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        # rowcount: ba'zan aniq bo'lmasligi mumkin, lekin tez
        inserted = max(cur.rowcount, 0)

    conn.commit()
    skipped = max(len(rows) - inserted, 0)
    return inserted, skipped


# ================== KEYWORDS ==================
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        return []
    data = json.loads(JOBS_PATH.read_text(encoding="utf-8"))
    return list({str(x).strip().lower() for x in data if str(x).strip()})


def normalize(s: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def match_keywords(
        title: Optional[str],
        company: Optional[str],
        location: Optional[str],
        skills: Optional[str],
        keywords: List[str],
) -> bool:
    # None bo‘lsa ham yiqilmasin
    hay = normalize(" ".join([(title or ""), (company or ""), (location or ""), (skills or "")]))

    if not keywords:
        # keyword list bo‘sh bo‘lsa — hammasini qo‘shib yuboramiz
        return True

    if not hay:
        return False

    for kw in keywords:
        tokens = normalize(kw).split()
        if any(t in hay for t in tokens if len(t) >= 2):
            return True
    return False


# ================== SELENIUM ==================
def create_driver():
    opts = webdriver.ChromeOptions()

    if HEADLESS:
        opts.add_argument("--headless=new")

    # window size
    try:
        width, height = [x.strip() for x in CHROME_WINDOW_SIZE.split(",")]
    except Exception:
        width, height = "1920", "1080"
    opts.add_argument(f"--window-size={width},{height}")

    # stable flags
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )

    driver.implicitly_wait(IMPLICIT_WAIT)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    return driver


def wait_ready(driver):
    WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def extract_rows(driver, page: int) -> List[Tuple]:
    rows = []
    trs = driver.find_elements(By.CSS_SELECTOR, "tr.job")

    for tr in trs:
        try:
            rid = tr.get_attribute("data-id")
            if not rid:
                continue

            job_id = f"remoteok_{rid}"

            # title/company
            title = None
            company = None
            h2 = tr.find_elements(By.CSS_SELECTOR, "h2")
            h3 = tr.find_elements(By.CSS_SELECTOR, "h3")
            if h2:
                title = safe_text(h2[0]) or None
            if h3:
                company = safe_text(h3[0]) or None

            # location/salary
            loc = None
            loc_els = tr.find_elements(By.CSS_SELECTOR, ".location")
            if loc_els:
                loc = safe_text(loc_els[0]) or None

            sal = None
            sal_els = tr.find_elements(By.CSS_SELECTOR, ".salary")
            if sal_els:
                sal = safe_text(sal_els[0]) or None

            # skills/tags
            tags = tr.find_elements(By.CSS_SELECTOR, ".tags a, .tags span")
            skills_list = [safe_text(t) for t in tags]
            skills_list = [x for x in skills_list if x]
            skills = ", ".join(skills_list) if skills_list else None

            # job_type from skills text
            job_type = None
            if skills:
                s = skills.lower()
                if "full" in s:
                    job_type = "Full-time"
                elif "part" in s:
                    job_type = "Part-time"
                elif "contract" in s:
                    job_type = "Contract"

            # job url
            link = None
            a_els = tr.find_elements(By.CSS_SELECTOR, "a[href*='/remote-jobs/']")
            if a_els:
                link = a_els[0].get_attribute("href") or None

            rows.append(
                (
                    job_id,  # 0
                    SOURCE_NAME,  # 1
                    title,  # 2
                    company,  # 3
                    loc,  # 4
                    sal,  # 5
                    job_type,  # 6
                    skills,  # 7
                    None,  # 8 education
                    link,  # 9
                    page,  # 10
                )
            )
        except Exception:
            continue

    return rows


# ================== SCRAPE LOOP (insert while scrolling) ==================
def scroll_collect_and_insert(driver, conn, keywords: List[str]) -> None:
    seen: Set[str] = set()
    no_new = 0

    total_unique = 0
    total_matched = 0
    total_inserted = 0
    total_skipped = 0

    for page in range(1, MAX_SCROLLS + 1):
        rows = extract_rows(driver, page)

        # only NEW ones for this page
        fresh = []
        for r in rows:
            if r[0] not in seen:
                seen.add(r[0])
                fresh.append(r)

        new_count = len(fresh)
        total_unique += new_count
        print(f"[SCROLL] page={page} total_unique={total_unique} new={new_count}")

        if new_count == 0:
            no_new += 1
        else:
            no_new = 0

        # filter
        filtered = [r for r in fresh if match_keywords(r[2], r[3], r[4], r[7], keywords)]
        matched = len(filtered)
        total_matched += matched

        if filtered:
            new_ins, skipped = insert_rows(conn, filtered)
            total_inserted += new_ins
            total_skipped += skipped
            print(f"[DB] page={page} matched={matched} inserted={new_ins} skipped={skipped}")
        else:
            print(f"[FILTER] page={page} matched=0")

        if no_new >= NO_NEW_LIMIT:
            print(f"[STOP] no_new_limit reached ({NO_NEW_LIMIT})")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

    print(
        f"[DONE] total_unique={total_unique} total_matched={total_matched} "
        f"total_inserted={total_inserted} total_skipped={total_skipped}"
    )


# ================== MAIN ==================
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

        scroll_collect_and_insert(driver, conn, keywords)

    finally:
        try:
            if driver:
                driver.quit()
        finally:
            conn.close()


if __name__ == "__main__":
    main()
