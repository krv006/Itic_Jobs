import os
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ---------------- PATHS ----------------
BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

# ---------------- SETTINGS ----------------
REMOTEOK_API_URL = os.getenv("REMOTEOK_API_URL", "https://remoteok.com/api")
SOURCE_NAME = "remoteok"

HTTP_TIMEOUT = 25


# ---------------- HELPERS ----------------
def _env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} yo‘q!")
    return val


def open_db():
    return psycopg2.connect(
        host=_env("DB_HOST"),
        port=int(_env("DB_PORT")),
        dbname=_env("DB_NAME"),
        user=_env("DB_USER"),
        password=_env("DB_PASSWORD"),
    )


def ensure_table(cur):
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


def load_keywords() -> List[str]:
    data = json.loads(JOBS_PATH.read_text(encoding="utf-8"))
    return [x.lower().strip() for x in data if str(x).strip()]


def fetch_jobs() -> List[Dict]:
    r = requests.get(
        REMOTEOK_API_URL,
        headers={"User-Agent": "Itic_Jobs/1.0"},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return [x for x in data[1:] if isinstance(x, dict)]


def match(job: Dict, keywords: List[str]) -> bool:
    text = " ".join(
        [
            str(job.get("position", "")),
            str(job.get("company", "")),
            str(job.get("location", "")),
        ]
    ).lower()

    tags = {t.lower() for t in job.get("tags", [])}

    return any(k in text or k in tags for k in keywords)


def detect_job_type(tags: List[str]) -> Optional[str]:
    t = {x.lower() for x in tags}
    if "full-time" in t:
        return "Full-time"
    if "contract" in t:
        return "Contract"
    if "freelance" in t:
        return "Freelance"
    return None


def salary_text(job: Dict) -> Optional[str]:
    smin = job.get("salary_min")
    smax = job.get("salary_max")
    if smin and smax:
        return f"{smin}-{smax}"
    return smin or smax


def to_row(job: Dict) -> Tuple:
    tags = job.get("tags", [])

    return (
        f"remoteok_{job['id']}",
        SOURCE_NAME,
        job.get("position"),
        job.get("company"),
        job.get("location") or "Remote",
        salary_text(job),
        detect_job_type(tags),
        ", ".join(tags) if tags else None,
        None,
        job.get("url"),
    )


def insert_jobs(conn, rows: List[Tuple]) -> int:
    sql = """
    INSERT INTO public.remoteok (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url
    )
    VALUES %s
    ON CONFLICT (job_id, source) DO NOTHING;
    """

    with conn.cursor() as cur:
        ensure_table(cur)

        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        before = cur.fetchone()[0]

        execute_values(cur, sql, rows, page_size=100)

        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        after = cur.fetchone()[0]

    conn.commit()
    return after - before


def main():
    keywords = load_keywords()
    print("[KEYWORDS]", keywords)

    jobs = fetch_jobs()
    filtered = [j for j in jobs if match(j, keywords)]
    print(f"[REMOTEOK] matched={len(filtered)}")

    rows = [to_row(j) for j in filtered]

    conn = open_db()
    try:
        new = insert_jobs(conn, rows)
        print(f"[DB] new_inserted={new}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
