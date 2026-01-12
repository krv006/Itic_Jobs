import datetime as dt
import json
import os
import time
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
import requests
from dotenv import load_dotenv

# =========================
# LOAD .env
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]  # Itic_Jobs
load_dotenv(BASE_DIR / ".env")

REMOTIVE_ENDPOINT = "https://remotive.com/api/remote-jobs"


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


def ensure_table_exists(conn: pyodbc.Connection) -> None:
    sql = """
    IF OBJECT_ID('dbo.remotive', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.remotive (
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
          CONSTRAINT PK_remotive PRIMARY KEY CLUSTERED (job_id)
        );
    END
    """
    conn.cursor().execute(sql)
    conn.commit()


def safe_text(x: Any, max_len: Optional[int] = None) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    if max_len is not None and len(s) > max_len:
        return s[:max_len]
    return s


# =========================
# JOB LIST
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
            f"Qidirilgan joylar:\n - {root_path}\n - {local_path}"
        )

    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list) or not all(isinstance(x, str) for x in data):
        raise RuntimeError(f"{p.name} list[str] bo‘lishi kerak")

    return [x.strip() for x in data if x and x.strip()]


# =========================
# REMOTIVE API
# =========================
def remotive_search(keyword: str, timeout: int = 30) -> List[Dict[str, Any]]:
    params = {"search": keyword}
    r = requests.get(REMOTIVE_ENDPOINT, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    jobs = data.get("jobs") or []
    return jobs if isinstance(jobs, list) else []


def parse_posted_date(x: Optional[str]) -> dt.date:
    if not x:
        return dt.date.today()
    try:
        return dt.datetime.fromisoformat(x.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return dt.date.fromisoformat(x[:10])
        except Exception:
            return dt.date.today()


def strip_html(html: str) -> str:
    if not html:
        return ""
    # juda oddiy html strip
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_skills(text: str, job_keywords: List[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    found = []
    for kw in job_keywords:
        if kw.lower() in low:
            found.append(kw)
    return ", ".join(sorted(set(found))) if found else None


def normalize_remotive_job(job: Dict[str, Any], job_keywords: List[str]) -> Dict[str, Any]:
    # Remotive ID int bo'ladi -> DB PK uchun doim string qilamiz
    rid = safe_text(job.get("id")) or safe_text(job.get("url")) or "unknown"
    job_id = f"remotive_{rid}"

    title = safe_text(job.get("title"), 100)
    company_name = safe_text(job.get("company_name"), 100)
    location = safe_text(job.get("candidate_required_location"), 100) or "Remote"

    description_html = safe_text(job.get("description")) or ""
    description = strip_html(description_html)

    skills = extract_skills(description, job_keywords)

    salary = safe_text(job.get("salary"))
    job_type = safe_text(job.get("job_type"), 50)
    job_subtitle = safe_text(job.get("category"), 250)

    job_url = safe_text(job.get("url"), 200)
    posted_date = parse_posted_date(safe_text(job.get("publication_date")))

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
        "source": "remotive",
        "description": description,
        "job_subtitle": job_subtitle,
        "posted_date": posted_date,
    }


# =========================
# SQL SERVER UPSERT -> dbo.remotive
# =========================
MERGE_SQL = """
MERGE dbo.remotive AS target
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

    cur = conn.cursor()
    cur.execute(MERGE_SQL, params_once + params_once)


# =========================
# RUNNER
# =========================
def run(sleep_sec: float = 0.6) -> Tuple[int, int]:
    keywords = load_job_list("job_list.json")
    conn = open_db()
    ensure_table_exists(conn)

    total_seen = 0
    total_upserted = 0

    try:
        for kw in keywords:
            print(f"\n=== KEYWORD: {kw} ===")
            jobs = remotive_search(kw)

            print(f"[RESULTS] {len(jobs)}")
            for j in jobs:
                total_seen += 1
                row = normalize_remotive_job(j, keywords)
                upsert(conn, row)
                total_upserted += 1

            conn.commit()

            # DB count ko'rsatib turamiz (real tushyaptimi yo'qmi)
            c = conn.cursor()
            c.execute("SELECT COUNT(1) FROM dbo.remotive")
            print("[DB COUNT]", c.fetchone()[0])

            time.sleep(sleep_sec)

        print(f"\n[DONE] total_seen={total_seen} upserted={total_upserted}")
        return total_seen, total_upserted

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run(sleep_sec=0.6)
