import datetime as dt
import json
import os
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import pyodbc
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

BASE_URL = "https://it-market.uz"
LIST_URL = f"{BASE_URL}/job/"

JOB_PATH_RE = re.compile(r"^/job/([A-Za-z0-9]+)/?$")


def load_env() -> Dict[str, str]:
    load_dotenv()  # .env
    cfg = {
        "driver": os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}"),
        "server": os.getenv("DB_SERVER"),
        "db_name": os.getenv("DB_NAME"),
        "trusted": os.getenv("DB_TRUSTED_CONNECTION", "yes"),
    }
    if not cfg["server"] or not cfg["db_name"]:
        raise RuntimeError("DB_SERVER yoki DB_NAME .env da yo'q yoki bo'sh.")
    return cfg


def load_keywords(path: str = "job_list.json") -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not all(isinstance(x, str) for x in data):
        raise RuntimeError("job_list.json list[str] bo'lishi kerak.")
    return data


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,uz;q=0.7",
        }
    )
    return s


def get_soup(session: requests.Session, url: str, params: Optional[dict] = None) -> BeautifulSoup:
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def extract_job_id(job_url: str) -> Optional[str]:
    path = urlparse(job_url).path.rstrip("/") + "/"
    m = re.match(r"^/job/([A-Za-z0-9]+)/$", path)
    return m.group(1) if m else None


def parse_list_for_job_links(soup: BeautifulSoup) -> List[str]:
    links = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href.startswith("/job/"):
            continue
        if "/apply" in href:
            continue

        m = JOB_PATH_RE.match(href.rstrip("/") + "/")
        if not m:
            continue

        links.append(urljoin(BASE_URL, href))

    seen = set()
    out = []
    for u in links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def collect_job_urls_for_keyword(session: requests.Session, keyword: str, max_pages: int = 50) -> Set[str]:
    all_urls: Set[str] = set()

    param_variants = [
        {"search": keyword},
        {"q": keyword},
        {"query": keyword},
    ]

    for params in param_variants:
        for page in range(1, max_pages + 1):
            p = dict(params)
            if page != 1:
                p["page"] = page

            soup = get_soup(session, LIST_URL, params=p)
            urls = parse_list_for_job_links(soup)
            if not urls:
                break

            before = len(all_urls)
            all_urls.update(urls)
            if len(all_urls) == before and page >= 2:
                break

    if not all_urls:
        for page in range(1, max_pages + 1):
            p = {} if page == 1 else {"page": page}
            soup = get_soup(session, LIST_URL, params=p)
            urls = parse_list_for_job_links(soup)
            if not urls:
                break
            all_urls.update(urls)

    return all_urls


def parse_label_value(lines: List[str], label: str) -> Optional[str]:
    if label not in lines:
        return None
    i = lines.index(label)
    stop = {
        "Work style", "Salary", "Work experience", "Employment type",
        "Description", "Tasks", "Schedule", "Required Skills", "Additional requirements"
    }
    for j in range(i + 1, min(i + 12, len(lines))):
        if lines[j] and lines[j] not in stop:
            return lines[j]
    return None


def parse_section_text(lines: List[str], section_name: str) -> Optional[str]:
    if section_name not in lines:
        return None
    i = lines.index(section_name)
    stop = {
        "Description", "Tasks", "Schedule", "Required Skills", "Additional requirements",
        "Work style", "Salary", "Work experience", "Employment type",
        "Minimum age", "Maximum age", "Comments",
    }
    buf = []
    for j in range(i + 1, len(lines)):
        t = lines[j]
        if t in stop:
            break
        if t:
            buf.append(t)
    return norm(" ".join(buf)) if buf else None


def parse_detail(job_url: str, soup: BeautifulSoup) -> Dict:
    job_id = extract_job_id(job_url) or job_url

    h1 = soup.select_one("h1")
    job_title = norm(h1.get_text(" ", strip=True)) if h1 else None

    lines = [norm(x) for x in soup.get_text("\n").split("\n")]
    lines = [x for x in lines if x]

    company_name = None
    if job_title and job_title in lines:
        idx = lines.index(job_title)
        for k in range(idx + 1, min(idx + 15, len(lines))):
            if lines[k] in {"General information about the employer", "Company Name"}:
                break
            if 2 <= len(lines[k]) <= 80:
                company_name = lines[k]
                break

    updated = None
    for t in lines:
        if t.startswith("Updated:"):
            updated = t.replace("Updated:", "").strip()
            break

    work_style = parse_label_value(lines, "Work style")
    salary = parse_label_value(lines, "Salary")
    work_experience = parse_label_value(lines, "Work experience")
    employment_type = parse_label_value(lines, "Employment type")

    description = parse_section_text(lines, "Description")
    tasks = parse_section_text(lines, "Tasks")
    schedule = parse_section_text(lines, "Schedule")
    add_req = parse_section_text(lines, "Additional requirements")

    required_skills = []
    if "Required Skills" in lines:
        i = lines.index("Required Skills")
        for j in range(i + 1, len(lines)):
            if lines[j] in {"Address:", "Phone:", "Email:", "Jobs by specializations", "Comments"}:
                break
            if 2 <= len(lines[j]) <= 80:
                required_skills.append(lines[j])
    rs_seen = set()
    required_skills = [x for x in required_skills if not (x in rs_seen or rs_seen.add(x))]

    posted_date = dt.date.today()
    if updated:
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                posted_date = dt.datetime.strptime(updated, fmt).date()
                break
            except ValueError:
                pass

    subtitle_parts = [x for x in [work_style, work_experience, employment_type] if x]
    job_subtitle = " | ".join(subtitle_parts) if subtitle_parts else None

    big_desc_parts = []
    if description:
        big_desc_parts.append("DESCRIPTION: " + description)
    if tasks:
        big_desc_parts.append("TASKS: " + tasks)
    if schedule:
        big_desc_parts.append("SCHEDULE: " + schedule)
    if add_req:
        big_desc_parts.append("ADDITIONAL REQUIREMENTS: " + add_req)
    full_description = "\n\n".join(big_desc_parts) if big_desc_parts else None

    return {
        "job_id": job_id,
        "job_title": job_title,
        "location": None,
        "skills": ", ".join(required_skills) if required_skills else None,
        "salary": salary,
        "education": None,
        "job_type": employment_type or work_style,
        "company_name": company_name,
        "job_url": job_url,
        "source": "it-market",
        "description": full_description,
        "job_subtitle": job_subtitle,
        "posted_date": posted_date,
    }


def keyword_match_text(text: str, keyword: str) -> bool:
    t = (text or "").lower()

    kw = keyword.lower().strip()

    if "/" in kw:
        parts = [p.strip() for p in kw.split("/") if p.strip()]
        return all(p in t for p in parts) or kw in t

    return kw in t


def job_matches_keyword(job: Dict, keyword: str) -> bool:
    hay = " ".join(
        [
            str(job.get("job_title") or ""),
            str(job.get("skills") or ""),
            str(job.get("description") or ""),
            str(job.get("job_subtitle") or ""),
            str(job.get("company_name") or ""),
        ]
    )
    return keyword_match_text(hay, keyword)


def get_conn(cfg: Dict[str, str]) -> pyodbc.Connection:
    conn = pyodbc.connect(
        f"Driver={cfg['driver']};"
        f"Server={cfg['server']};"
        f"Database={cfg['db_name']};"
        f"Trusted_Connection={cfg['trusted']};"
    )
    return conn


def save_to_db(cursor: pyodbc.Cursor, job: Dict) -> bool:
    sql = """
    INSERT INTO it_park (
        job_id, job_title, location, skills, salary, education, job_type,
        company_name, job_url, source, description, job_subtitle, posted_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor.execute(
            sql,
            (
                job["job_id"],
                job["job_title"],
                job["location"],
                job["skills"],
                job["salary"],
                job["education"],
                job["job_type"],
                job["company_name"],
                job["job_url"],
                job["source"],
                job["description"],
                job["job_subtitle"],
                job["posted_date"],
            ),
        )
        return True
    except pyodbc.IntegrityError:
        return False


def run(max_pages_per_keyword: int = 40):
    cfg = load_env()
    keywords = load_keywords("job_list.json")

    session = make_session()
    conn = get_conn(cfg)
    cursor = conn.cursor()

    seen_ids: Set[str] = set()
    inserted = 0
    duplicates = 0
    matched = 0

    for kw in keywords:
        print(f"\n=== KEYWORD: {kw} ===")
        urls = collect_job_urls_for_keyword(session, kw, max_pages=max_pages_per_keyword)
        print(f"[LIST] collected_urls={len(urls)}")

        for url in sorted(urls):
            jid = extract_job_id(url) or url
            if jid in seen_ids:
                continue
            seen_ids.add(jid)

            try:
                dsoup = get_soup(session, url)
                job = parse_detail(url, dsoup)
            except Exception as e:
                print(f"[WARN] detail error: {url} -> {e}")
                continue

            if not job_matches_keyword(job, kw):
                continue

            matched += 1
            ok = save_to_db(cursor, job)
            if ok:
                inserted += 1
                conn.commit()
                print(f"[SAVE] {job['job_id']} | {job.get('job_title')}")
            else:
                duplicates += 1

    cursor.close()
    conn.close()

    print("\nDONE")
    print(f"matched={matched} inserted={inserted} duplicates={duplicates} total_seen={len(seen_ids)}")


if __name__ == "__main__":
    run(max_pages_per_keyword=40)


"""
CREATE TABLE [dbo].[it_park ](
  [job_id] [nvarchar](100) NOT NULL,
  [job_title] [nvarchar](100) NULL,
  [location] [nvarchar](100) NULL,
  [skills] [nvarchar](max) NULL,
  [salary] [nvarchar](max) NULL,
  [education] [nvarchar](100) NULL,
  [job_type] [nvarchar](50) NULL,
  [company_name] [nvarchar](100) NULL,
  [job_url] [nvarchar](200) NULL,
  [source] [nvarchar](20) NULL,
  [description] [nvarchar](max) NULL,
  [job_subtitle] [nvarchar](250) NULL,
[posted_date] DATE NOT NULL, 
PRIMARY KEY CLUSTERED 
(
  [job_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

"""