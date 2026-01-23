import datetime
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
    """
    ✅ New columns:
    - posted_at (real time from '7h/2d/1mo')
    - posted_date
    - job_subtitle (matched keyword)
    """
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
        posted_at TIMESTAMP NULL,
        posted_date DATE NULL,
        job_subtitle TEXT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE (job_id, source)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        # safety for existing tables
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS posted_at TIMESTAMP NULL;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS posted_date DATE NULL;")
        cur.execute("ALTER TABLE public.remoteok ADD COLUMN IF NOT EXISTS job_subtitle TEXT NULL;")
    conn.commit()


def insert_rows(conn, rows: List[Tuple]) -> Tuple[int, int]:
    """
    returns: (inserted_or_updated, skipped_estimate)
    """
    if not rows:
        return 0, 0

    sql = """
    INSERT INTO public.remoteok (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url, page,
        posted_at, posted_date, job_subtitle
    )
    VALUES %s
    ON CONFLICT (job_id, source) DO UPDATE SET
        job_title    = EXCLUDED.job_title,
        company_name = EXCLUDED.company_name,
        location     = EXCLUDED.location,
        salary       = EXCLUDED.salary,
        job_type     = EXCLUDED.job_type,
        skills       = EXCLUDED.skills,
        education    = EXCLUDED.education,
        job_url      = EXCLUDED.job_url,
        page         = EXCLUDED.page,
        posted_at    = COALESCE(EXCLUDED.posted_at, public.remoteok.posted_at),
        posted_date  = COALESCE(EXCLUDED.posted_date, public.remoteok.posted_date),
        job_subtitle = COALESCE(EXCLUDED.job_subtitle, public.remoteok.job_subtitle);
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=300)
    conn.commit()

    # estimate: we can't easily know exact updated vs inserted without RETURNING,
    # so we return len(rows) as processed and 0 skipped.
    return len(rows), 0


# ================== KEYWORDS ==================
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        return []
    data = json.loads(JOBS_PATH.read_text(encoding="utf-8"))
    # keep original + lower version for matching
    kws = []
    for x in data:
        s = str(x).strip()
        if s:
            kws.append(s)
    # unique preserve order
    seen = set()
    out = []
    for k in kws:
        kl = k.lower()
        if kl not in seen:
            seen.add(kl)
            out.append(k)
    return out


def normalize(s: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def find_matching_keyword(
    title: Optional[str],
    company: Optional[str],
    location: Optional[str],
    skills: Optional[str],
    keywords: List[str],
) -> Optional[str]:
    """
    ✅ returns the FIRST keyword that matches (job_subtitle)
    """
    hay = normalize(" ".join([(title or ""), (company or ""), (location or ""), (skills or "")]))
    if not hay or not keywords:
        return None

    for kw in keywords:
        tokens = normalize(kw).split()
        if any(t in hay for t in tokens if len(t) >= 2):
            return kw  # original keyword text
    return None


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


# ================== POSTED TIME PARSER (7h / 2d / 1mo ...)
def parse_relative_age(age_text: str) -> Optional[datetime.datetime]:
    """
    RemoteOK cards show: '7h', '2d', '1mo', '3y' (sometimes 'm' minutes)
    We convert to posted_at = now - delta
    """
    if not age_text:
        return None

    t = age_text.strip().lower()
    t = t.replace(" ", "")

    now = datetime.datetime.now()

    # Examples: 7h, 2d, 1mo, 3y, 45m
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
        # approx month = 30 days
        return now - datetime.timedelta(days=30 * n)
    if unit == "y":
        # approx year = 365 days
        return now - datetime.timedelta(days=365 * n)

    return None


def extract_posted_at_from_tr(tr) -> Optional[datetime.datetime]:
    """
    Try to find card time text (e.g. '7h') from the row.
    RemoteOK layout can vary, so we try multiple selectors.
    """
    # Most common: time text in a time cell
    selectors = [
        "td.time",           # often contains '7h'
        ".time",             # fallback
        "time",              # sometimes time tag
    ]

    for sel in selectors:
        try:
            el = tr.find_element(By.CSS_SELECTOR, sel)
            txt = safe_text(el)
            dt = parse_relative_age(txt)
            if dt:
                return dt
        except Exception:
            continue

    # try attribute-based (sometimes <time datetime="...">)
    try:
        t_el = tr.find_element(By.CSS_SELECTOR, "time[datetime]")
        dt_raw = t_el.get_attribute("datetime")
        if dt_raw:
            # try ISO parsing
            try:
                # keep simple: YYYY-MM-DDTHH:MM:SSZ
                dt_raw = dt_raw.replace("Z", "+00:00")
                return datetime.datetime.fromisoformat(dt_raw).replace(tzinfo=None)
            except Exception:
                pass
    except Exception:
        pass

    return None


def extract_rows(driver, page: int) -> List[Tuple]:
    """
    Base rows WITHOUT job_subtitle (it’s added after keyword match)
    Tuple layout (base):
      job_id, source, title, company, loc, sal, job_type, skills, education, link, page, posted_at, posted_date
    """
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

            posted_at = extract_posted_at_from_tr(tr)
            posted_date = posted_at.date() if posted_at else None

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
                    None,  # education
                    link,
                    page,
                    posted_at,
                    posted_date,
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
    total_processed = 0

    for page in range(1, MAX_SCROLLS + 1):
        base_rows = extract_rows(driver, page)

        # uniq
        fresh = []
        for r in base_rows:
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

        # filter + attach job_subtitle (matched keyword)
        filtered_with_subtitle = []
        for r in fresh:
            job_id, source, title, company, loc, sal, job_type, skills, edu, link, pg, posted_at, posted_date = r
            matched_kw = find_matching_keyword(title, company, loc, skills, keywords)
            if not matched_kw:
                continue

            filtered_with_subtitle.append(
                (
                    job_id, source, title, company, loc, sal, job_type, skills, edu, link, pg,
                    posted_at, posted_date, matched_kw
                )
            )

        if filtered_with_subtitle:
            processed, _ = insert_rows(conn, filtered_with_subtitle)
            total_processed += processed
            print(f"[DB] page={page} matched={len(filtered_with_subtitle)} upserted={processed}")
        else:
            print(f"[FILTER] page={page} matched=0")

        if no_new >= NO_NEW_LIMIT:
            print(f"[STOP] no_new_limit reached ({NO_NEW_LIMIT})")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

    print(f"[DONE] total_unique={total_unique} total_upserted={total_processed}")


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
