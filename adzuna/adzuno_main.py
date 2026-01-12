import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# LOAD .env
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]  # Itic_Jobs
load_dotenv(BASE_DIR / ".env")


# =========================
# ENV + DB
# =========================
def _env_required(key: str) -> str:
    val = os.getenv(key)
    if val is None or str(val).strip() == "":
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return str(val).strip()


def open_db() -> pyodbc.Connection:
    driver = _env_required("DB_DRIVER")
    server = _env_required("DB_SERVER")
    db_name = _env_required("DB_NAME")

    trusted = os.getenv("DB_TRUSTED_CONNECTION", "yes").strip().lower()
    trusted = "yes" if trusted in ("1", "true", "yes", "y") else "no"

    driver = driver.strip().strip("{}").strip().strip('"').strip("'")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={db_name};"
        f"Trusted_Connection={trusted};"
    )

    print("[DB]", conn_str)
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def safe_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


# =========================
# TABLE ENSURE (optional, lekin foydali)
# =========================
def ensure_table_exists(conn: pyodbc.Connection) -> None:
    sql = """
    IF OBJECT_ID('dbo.adzune', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.adzune (
          job_id NVARCHAR(100) NOT NULL,
          job_title NVARCHAR(100) NULL,
          location NVARCHAR(100) NULL,
          skills NVARCHAR(MAX) NULL,
          salary NVARCHAR(MAX) NULL,
          education NVARCHAR(100) NULL,
          job_type NVARCHAR(50) NULL,
          company_name NVARCHAR(100) NULL,
          job_url NVARCHAR(200) NULL,
          source NVARCHAR(20) NULL,
          description NVARCHAR(MAX) NULL,
          job_subtitle NVARCHAR(250) NULL,
          posted_date DATE NOT NULL,
          CONSTRAINT PK_adzune PRIMARY KEY CLUSTERED (job_id)
        );
    END
    """
    conn.cursor().execute(sql)
    conn.commit()


# =========================
# CHROME DRIVER (global)
# =========================
_driver: Optional[webdriver.Chrome] = None


def get_driver() -> webdriver.Chrome:
    global _driver
    if _driver is None:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        # options.add_argument("--headless=new")  # xohlasangiz fon rejimida

        _driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
    return _driver


# =========================
# SOURCE: RemoteOK HTML (Chrome bilan)
# =========================
def _slugify(q: str) -> str:
    return (
        q.lower()
        .replace("/", "-")
        .replace(" ", "-")
        .replace("--", "-")
    )


def fetch_jobs_for_keyword(keyword: str, page: int, where: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Chrome ochadi va RemoteOK HTML sahifasidan joblarni oladi.
    RemoteOK pagination yo'q -> page>1 bo'lsa stop.
    """
    if page > 1:
        return []

    driver = get_driver()
    url = f"https://remoteok.com/remote-{_slugify(keyword)}-jobs"
    driver.get(url)
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    jobs: List[Dict[str, Any]] = []

    for row in soup.select("tr.job"):
        # RemoteOK job id
        raw_id = row.get("data-id") or ""
        link = row.select_one("a.preventLink")
        if not link:
            continue

        href = link.get("href", "")
        if not href:
            continue

        job_url = "https://remoteok.com" + href
        title_el = row.select_one("h2")
        comp_el = row.select_one("h3")

        title = title_el.get_text(strip=True) if title_el else keyword
        company = comp_el.get_text(strip=True) if comp_el else "Unknown"

        jobs.append({
            "id": f"remoteok_{raw_id or job_url}",
            "title": title,
            "location": {"display_name": "Remote"},
            "company": {"display_name": company},
            "description": "",  # xohlasangiz detail page kirib description ham olamiz
            "redirect_url": job_url,
            "url": job_url,
            "created": None,
            "contract_time": None,
            "contract_type": None,
            "category": {"label": "RemoteOK"},
            "salary_min": None,
            "salary_max": None,
            "salary_is_predicted": None,
            "_source": "remoteok",
        })

    return jobs


# =========================
# NORMALIZE
# =========================
def parse_posted_date(created: Optional[str]) -> dt.date:
    if not created:
        return dt.date.today()
    try:
        created = created.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(created).date()
    except Exception:
        return dt.date.today()


def build_salary(job: Dict[str, Any]) -> Optional[str]:
    salary_min = job.get("salary_min")
    salary_max = job.get("salary_max")
    predicted = job.get("salary_is_predicted")

    if salary_min is None and salary_max is None:
        return None

    parts = []
    if salary_min is not None:
        parts.append(f"min={salary_min}")
    if salary_max is not None:
        parts.append(f"max={salary_max}")
    if predicted is not None:
        parts.append(f"predicted={predicted}")
    return "; ".join(parts)


def extract_skills(description: str, job_keywords: List[str]) -> Optional[str]:
    if not description:
        return None
    low = description.lower()
    found = []
    for kw in job_keywords:
        if kw.lower() in low:
            found.append(kw)
    return ", ".join(sorted(set(found))) if found else None


def normalize_job(job: Dict[str, Any], job_keywords: List[str]) -> Dict[str, Any]:
    job_id = safe_text(job.get("id"))
    title = safe_text(job.get("title"))

    location = None
    loc = job.get("location") or {}
    if isinstance(loc, dict):
        location = safe_text(loc.get("display_name"))

    company_name = None
    comp = job.get("company") or {}
    if isinstance(comp, dict):
        company_name = safe_text(comp.get("display_name"))

    description = safe_text(job.get("description")) or ""
    skills = extract_skills(description, job_keywords)

    salary = build_salary(job)

    job_type = safe_text(job.get("contract_time")) or safe_text(job.get("contract_type"))

    job_subtitle = None
    cat = job.get("category")
    if isinstance(cat, dict):
        job_subtitle = safe_text(cat.get("label"))

    job_url = safe_text(job.get("redirect_url")) or safe_text(job.get("url"))
    posted_date = parse_posted_date(safe_text(job.get("created")))

    return {
        "job_id": job_id,
        "job_title": title,
        "location": location,
        "skills": skills,
        "salary": salary,
        "education": None,
        "job_type": job_type,
        "company_name": company_name,
        "job_url": job_url,
        "source": safe_text(job.get("_source")) or "unknown",
        "description": description,
        "job_subtitle": job_subtitle,
        "posted_date": posted_date,
    }


# =========================
# SQL SERVER UPSERT
# =========================
MERGE_SQL = """
MERGE dbo.adzune AS target
USING (SELECT ? AS job_id) AS src
ON target.job_id = src.job_id
WHEN MATCHED THEN
  UPDATE SET
    job_title = ?,
    location = ?,
    skills = ?,
    salary = ?,
    education = ?,
    job_type = ?,
    company_name = ?,
    job_url = ?,
    source = ?,
    description = ?,
    job_subtitle = ?,
    posted_date = ?
WHEN NOT MATCHED THEN
  INSERT (
    job_id, job_title, location, skills, salary, education, job_type,
    company_name, job_url, source, description, job_subtitle, posted_date
  )
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def upsert(conn: pyodbc.Connection, row: Dict[str, Any]) -> None:
    if not row.get("job_id"):
        return

    params_once = (
        row["job_id"],
        row["job_title"],
        row["location"],
        row["skills"],
        row["salary"],
        row["education"],
        row["job_type"],
        row["company_name"],
        row["job_url"],
        row["source"],
        row["description"],
        row["job_subtitle"],
        row["posted_date"],
    )
    params = params_once + params_once
    conn.cursor().execute(MERGE_SQL, params)


# =========================
# RUNNER
# =========================
def load_job_list(path: str = "job_list.json") -> List[str]:
    root_path = BASE_DIR / path
    local_path = Path(__file__).resolve().parent / path

    if root_path.exists():
        p = root_path
    elif local_path.exists():
        p = local_path
    else:
        raise FileNotFoundError(
            f"job_list.json topilmadi!\n"
            f"Qidirilgan joylar:\n"
            f" - {root_path}\n"
            f" - {local_path}\n"
            f"Iltimos faylni shu joylardan biriga qo‘ying."
        )

    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list) or not all(isinstance(x, str) for x in data):
        raise RuntimeError(f"{p.name} list[str] bo‘lishi kerak")

    return [x.strip() for x in data if x and x.strip()]


def run(max_pages_per_keyword: int = 10, where: Optional[str] = None, sleep_sec: float = 1.0) -> Tuple[int, int]:
    keywords = load_job_list("job_list.json")

    conn = open_db()
    ensure_table_exists(conn)

    total_seen = 0
    total_upserted = 0

    try:
        for kw in keywords:
            print(f"\n=== KEYWORD: {kw} ===")

            for page in range(1, max_pages_per_keyword + 1):
                jobs = fetch_jobs_for_keyword(kw, page, where=where)

                if not jobs:
                    print(f"[STOP] keyword='{kw}' page={page} -> results=0")
                    break

                print(f"[PAGE] {page} results={len(jobs)}")
                for job in jobs:
                    total_seen += 1
                    row = normalize_job(job, keywords)
                    if not row["job_id"]:
                        continue
                    upsert(conn, row)
                    total_upserted += 1

                conn.commit()
                time.sleep(sleep_sec)

        print(f"\n[DONE] total_seen={total_seen} upserted={total_upserted}")
        return total_seen, total_upserted

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run(max_pages_per_keyword=10, where=None, sleep_sec=1.0)
    finally:
        if _driver is not None:
            _driver.quit()
