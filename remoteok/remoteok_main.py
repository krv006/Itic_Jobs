import os
import json
import time
import re
from pathlib import Path
from typing import List, Tuple, Set, Optional

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ================== CONFIG ==================
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

REMOTEOK_URL = "https://remoteok.com/"
SOURCE_NAME = "remoteok"

MAX_SCROLLS = 80
SCROLL_PAUSE = 1.2
NO_NEW_LIMIT = 6
HEADLESS = False


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
    returns: (new_inserted, skipped)
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
        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        before = cur.fetchone()[0]

        execute_values(cur, sql, rows, page_size=300)

        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        after = cur.fetchone()[0]

    conn.commit()
    new_inserted = after - before
    skipped = max(len(rows) - new_inserted, 0)
    return new_inserted, skipped


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

    if not hay or not keywords:
        return False

    for kw in keywords:
        tokens = normalize(kw).split()
        # tokenlar ichidan kamida bittasi matnda bo‘lsa True
        if any(t in hay for t in tokens if len(t) >= 2):
            return True
    return False


# ================== SELENIUM ==================
def create_driver():
    opts = webdriver.ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )


def wait_ready(driver):
    WebDriverWait(driver, 30).until(
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
            title = safe_text(tr.find_element(By.CSS_SELECTOR, "h2")) or None
            company = safe_text(tr.find_element(By.CSS_SELECTOR, "h3")) or None

            loc = None
            loc_els = tr.find_elements(By.CSS_SELECTOR, ".location")
            if loc_els:
                loc = safe_text(loc_els[0]) or None

            sal = None
            sal_els = tr.find_elements(By.CSS_SELECTOR, ".salary")
            if sal_els:
                sal = safe_text(sal_els[0]) or None

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

            rows.append(
                (
                    job_id,
                    SOURCE_NAME,
                    title,
                    company,
                    loc,
                    sal,
                    job_type,
                    skills,
                    None,   # education
                    link,
                    page,
                )
            )
        except Exception:
            continue
    return rows


# ================== MAIN LOOP (insert while scrolling) ==================
def scroll_collect_and_insert(driver, conn, keywords: List[str]) -> None:
    seen: Set[str] = set()
    no_new = 0
    total_unique = 0
    total_inserted = 0
    total_skipped = 0

    for page in range(1, MAX_SCROLLS + 1):
        rows = extract_rows(driver, page)

        # uniq qilib olamiz
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

        # shu pagedagi yangi rowlarni filter qilib darhol DB ga yozamiz
        filtered = [r for r in fresh if match_keywords(r[2], r[3], r[4], r[7], keywords)]

        if filtered:
            new_ins, skipped = insert_rows(conn, filtered)
            total_inserted += new_ins
            total_skipped += skipped
            print(f"[DB] page={page} matched={len(filtered)} inserted={new_ins} skipped={skipped}")
        else:
            print(f"[FILTER] page={page} matched=0")

        if no_new >= NO_NEW_LIMIT:
            print(f"[STOP] no_new_limit reached ({NO_NEW_LIMIT})")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

    print(f"[DONE] total_unique={total_unique} total_inserted={total_inserted} total_skipped={total_skipped}")


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
