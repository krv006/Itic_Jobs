# hitmarker_main.py
import os
import re
import json
import time
import hashlib
from pathlib import Path
from typing import Optional, List, Dict

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from bs4 import BeautifulSoup

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ---------------- CONFIG ----------------
load_dotenv()

BASE_URL = "https://hitmarker.net"
LIST_URL_TMPL = BASE_URL + "/jobs?page={page}"
SOURCE_NAME = "hitmarker"

MAX_PAGES = int(os.getenv("HITMARKER_MAX_PAGES", "50"))
REQUEST_TIMEOUT = int(os.getenv("HITMARKER_TIMEOUT", "25"))
SLEEP_BETWEEN_PAGES = float(os.getenv("HITMARKER_SLEEP", "0.6"))
SLEEP_BETWEEN_JOBS = float(os.getenv("HITMARKER_JOB_SLEEP", "0.15"))

# Agar list page’da ketma-ket shu miqdorda "new=0" bo‘lsa STOP
NO_NEW_PAGES_STOP = int(os.getenv("HITMARKER_NO_NEW_STOP", "3"))

HEADLESS = os.getenv("HEADLESS", "false").strip().lower() in ("1", "true", "yes", "y")
CHROME_VERSION_MAIN = os.getenv("CHROME_VERSION_MAIN")  # optional (masalan: 120)

# jobs_list.json path (TO‘G‘RI: Path bo‘lishi shart)
BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "jobs_list.json")))

UA = os.getenv(
    "HITMARKER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# Job detail linklari odatda shunaqa: https://hitmarker.net/jobs/slug-12345
JOB_URL_RE = re.compile(r"^https?://(www\.)?hitmarker\.net/jobs/.+-\d+$", re.I)


# ---------------- DB ----------------
def env_required(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return v

def get_pg_conn():
    return psycopg2.connect(
        host=env_required("DB_HOST"),
        port=env_required("DB_PORT"),
        dbname=env_required("DB_NAME"),
        user=env_required("DB_USER"),
        password=env_required("DB_PASSWORD"),
    )

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS hitmarker_jobs (
    id SERIAL PRIMARY KEY,
    job_hash CHAR(64) NOT NULL UNIQUE,

    title TEXT,
    company TEXT,
    location TEXT,

    employment_type TEXT,
    experience_level TEXT,
    salary TEXT,

    job_url TEXT,
    description TEXT,

    source TEXT DEFAULT 'hitmarker',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hitmarker_company ON hitmarker_jobs(company);
CREATE INDEX IF NOT EXISTS idx_hitmarker_location ON hitmarker_jobs(location);
CREATE INDEX IF NOT EXISTS idx_hitmarker_created_at ON hitmarker_jobs(created_at);
"""

INSERT_SQL = """
INSERT INTO hitmarker_jobs (
    job_hash, title, company, location,
    employment_type, experience_level, salary,
    job_url, description, source
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (job_hash) DO NOTHING;
"""

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


# ---------------- KEYWORDS ----------------
def load_keywords() -> List[str]:
    """
    jobs_list.json formatlari:
    - ["ai", "vfx", ...]
    - {"keywords":[...]}
    - {"KEYWORDS":[...]}
    """
    if not JOBS_PATH.exists():
        print(f"[WARN] jobs_list.json topilmadi: {JOBS_PATH} -> filter o‘chadi")
        return []

    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        kws = data
    elif isinstance(data, dict):
        kws = data.get("keywords") or data.get("KEYWORDS") or []
    else:
        kws = []

    kws = [str(x).strip().lower() for x in kws if str(x).strip()]

    seen = set()
    out = []
    for k in kws:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out

def matches_keywords(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    t = (text or "").lower()
    return any(k in t for k in keywords)


# ---------------- HASH ----------------
def job_hash(title: str, company: str, location: str, url: str) -> str:
    raw = f"{title}|{company}|{location}|{url}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------- HTTP (DETAIL PAGES via requests) ----------------
def http_get(url: str, retries: int = 3) -> str:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": BASE_URL + "/jobs",
    }

    last = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            sleep_s = 1.2 * attempt
            print(f"[HTTP] retry {attempt}/{retries} url={url} err={e} sleep={sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed: {url} last_error={last}")


# ---------------- SELENIUM (LIST PAGES) ----------------
def create_driver():
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument(f"--user-agent={UA}")

    version_main = int(CHROME_VERSION_MAIN) if CHROME_VERSION_MAIN and CHROME_VERSION_MAIN.isdigit() else None
    return uc.Chrome(options=options, version_main=version_main)

def collect_job_urls_selenium(max_pages: int) -> List[str]:
    driver = create_driver()
    wait = WebDriverWait(driver, 25)

    seen = set()
    urls: List[str] = []

    no_new_pages = 0

    try:
        for page in range(1, max_pages + 1):
            url = LIST_URL_TMPL.format(page=page)
            driver.get(url)

            # Sahifada job linklar paydo bo‘lishini kutamiz
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/jobs/"]')))
            time.sleep(1.0)  # DOM stabil bo‘lsin

            anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/jobs/"]')
            found = 0
            new = 0

            for a in anchors:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                if JOB_URL_RE.match(href):
                    found += 1
                    if href not in seen:
                        seen.add(href)
                        urls.append(href)
                        new += 1

            print(f"[LIST] page={page} found_links={found} new={new} total={len(urls)}")

            if new == 0:
                no_new_pages += 1
            else:
                no_new_pages = 0

            if no_new_pages >= NO_NEW_PAGES_STOP:
                print(f"[STOP] no new urls for {NO_NEW_PAGES_STOP} pages")
                break

            time.sleep(SLEEP_BETWEEN_PAGES)

    finally:
        driver.quit()

    return urls


# ---------------- PARSE DETAIL ----------------
EMP_HINTS = ("Full Time", "Part Time", "Contract", "Freelance", "Internship", "Temporary")

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_job_detail(html: str, job_url: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = clean(h1.get_text()) if h1 else None

    # Company (ko‘pincha /companies/ link)
    company = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = clean(a.get_text())
        if txt and href.startswith("/companies/"):
            company = txt
            break
    if company is None and h1:
        for a in h1.find_all_next("a", href=True, limit=8):
            txt = clean(a.get_text())
            if txt:
                company = txt
                break

    # Page text heuristika
    lines = [clean(x) for x in soup.get_text("\n").split("\n")]
    lines = [x for x in lines if x]

    employment_type = None
    experience_level = None
    salary = None
    location = None

    # employment type
    for line in lines:
        if any(h in line for h in EMP_HINTS) and len(line) <= 80:
            employment_type = line
            break

    # experience
    for line in lines:
        low = line.lower()
        if "years" in low and "(" in line and ")" in line and len(line) <= 100:
            experience_level = line
            break

    # salary
    for line in lines:
        low = line.lower()
        if (("$" in line) or ("£" in line) or ("€" in line) or ("per year" in low) or ("per hour" in low)) and len(line) <= 120:
            salary = line
            break

    # location
    for line in lines:
        if ("Remote" in line) and len(line) <= 80:
            location = line
            break
    if location is None:
        for line in lines:
            if "," in line and len(line) <= 80 and line not in (title or "") and line not in (company or ""):
                if line.lower() not in ("jobs", "companies", "news", "about", "contact", "report"):
                    location = line
                    break

    # Description (limit 20k)
    description = clean(soup.get_text(" "))[:20000] if soup else None

    return {
        "title": title,
        "company": company,
        "location": location,
        "employment_type": employment_type,
        "experience_level": experience_level,
        "salary": salary,
        "job_url": job_url,
        "description": description,
    }


# ---------------- PIPELINE ----------------
def insert_jobs(conn, jobs: List[Dict[str, Optional[str]]]):
    if not jobs:
        print("[DB] no jobs to insert")
        return

    rows = []
    for j in jobs:
        rows.append(
            (
                j["job_hash"],
                j.get("title"),
                j.get("company"),
                j.get("location"),
                j.get("employment_type"),
                j.get("experience_level"),
                j.get("salary"),
                j.get("job_url"),
                j.get("description"),
                j.get("source"),
            )
        )

    with conn.cursor() as cur:
        execute_batch(cur, INSERT_SQL, rows, page_size=200)
    conn.commit()

def main():
    keywords = load_keywords()
    print(f"[DEBUG] JOBS_PATH={JOBS_PATH}")
    print(f"[KEYWORDS] {len(keywords)} -> {keywords[:12]}{'...' if len(keywords) > 12 else ''}")

    print(f"[START] source={SOURCE_NAME} list_pages={MAX_PAGES} headless={HEADLESS}")

    job_urls = collect_job_urls_selenium(MAX_PAGES)
    print(f"[LIST DONE] total_urls={len(job_urls)}")

    jobs: List[Dict[str, Optional[str]]] = []
    matched = 0

    for idx, url in enumerate(job_urls, start=1):
        html = http_get(url)
        data = parse_job_detail(html, url)

        title = data.get("title") or ""
        desc = data.get("description") or ""

        if not matches_keywords(title + " " + desc, keywords):
            if idx % 50 == 0:
                print(f"[DETAIL] {idx}/{len(job_urls)} matched={matched}")
            time.sleep(SLEEP_BETWEEN_JOBS)
            continue

        matched += 1
        data["job_hash"] = job_hash(
            data.get("title") or "",
            data.get("company") or "",
            data.get("location") or "",
            url,
        )
        data["source"] = SOURCE_NAME
        jobs.append(data)

        if idx % 25 == 0:
            print(f"[DETAIL] {idx}/{len(job_urls)} matched={matched}")

        time.sleep(SLEEP_BETWEEN_JOBS)

    print(f"[DETAIL DONE] total_matched_jobs={len(jobs)}")

    conn = get_pg_conn()
    try:
        ensure_table(conn)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hitmarker_jobs;")
            before = cur.fetchone()[0]

        insert_jobs(conn, jobs)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hitmarker_jobs;")
            after = cur.fetchone()[0]

        inserted = after - before
        print(f"[DB] before={before} after={after} inserted={inserted} skipped~={len(jobs) - inserted}")

    finally:
        conn.close()

    print("[DONE]")


if __name__ == "__main__":
    main()
