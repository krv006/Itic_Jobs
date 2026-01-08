import datetime as dt
import json
import os
import re
import time
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import pyodbc
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv

BASE_URL = "https://it-market.uz"
LIST_URL = f"{BASE_URL}/job/"
JOB_PATH_RE = re.compile(r"^/job/([A-Za-z0-9]+)/?$")

META_LABELS = ["Work style", "Salary", "Work experience", "Employment type", "Location"]
SECTION_LABELS = ["Description", "Tasks", "Schedule", "Required Skills", "Additional requirements"]

WORK_STYLES = ["Office Work", "Remote Work", "Partially Remote Work"]
EMP_TYPES = ["Full Time", "Part Time", "To be discussed"]
EXPERIENCES = ["No experience", "From 1 to 3 years", "From 3 to 5 years", "Over 5 years"]

SALARY_RE = re.compile(
    r"(?P<salary>\b\d[\d\s]*\s*(?:UZS|USD)\s*dan\s*(?:boshlab|\d[\d\s]*\s*gacha)\b)",
    re.IGNORECASE
)

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
URL_RE = re.compile(r"(https?://\S+|www\.\S+)", re.I)

BAD_SKILL_EXACT = {
    "IT Market", "Companies", "Specialists", "Jobs", "Orders", "Contact us", "Submit an application",
    "Work style", "Salary", "Work experience", "Employment type", "Location",
    "Description", "Tasks", "Schedule", "Required Skills", "Additional requirements",
    "Company Name", "General information about the employer",
    "Minimum age", "Maximum age", "Comments",
    "Address:", "Phone:", "Email:", "Jobs by specializations",
}
META_VALUES_TO_EXCLUDE_FROM_SKILLS = set(WORK_STYLES + EMP_TYPES + EXPERIENCES)


# -------------------------
# ENV + KEYWORDS
# -------------------------
def load_env() -> Dict[str, str]:
    load_dotenv()
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
    return [x.strip() for x in data if x.strip()]


# -------------------------
# HTTP (✅ retry + backoff)
# -------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,uz;q=0.7",
            "Connection": "keep-alive",
        }
    )
    return s


def get_soup(
        session: requests.Session,
        url: str,
        params: Optional[dict] = None,
        retries: int = 4,
        timeout: tuple = (10, 45),  # (connect, read)
) -> Optional[BeautifulSoup]:
    """
    - 404 -> None
    - Timeout/429/5xx -> retry + backoff
    - baribir bo'lmasa -> None (script yiqilmaydi)
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)

            if r.status_code == 404:
                return None

            # 429 / 5xx -> retry
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
                sleep_s = min(20, 2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue

            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")

        except requests.exceptions.ReadTimeout as e:
            last_err = e
            time.sleep(min(20, 2 ** (attempt - 1)))
        except requests.exceptions.ConnectionError as e:
            last_err = e
            time.sleep(min(20, 2 ** (attempt - 1)))
        except requests.exceptions.RequestException as e:
            # boshqa error bo'lsa ham yiqitmaymiz
            last_err = e
            break

    print(f"[WARN] get_soup failed: {url} err={last_err}")
    return None


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def extract_job_id(job_url: str) -> Optional[str]:
    path = urlparse(job_url).path.rstrip("/") + "/"
    m = re.match(r"^/job/([A-Za-z0-9]+)/$", path)
    return m.group(1) if m else None


def lines_from(root: Tag) -> List[str]:
    lines = [norm(x) for x in root.get_text("\n").split("\n")]
    return [x for x in lines if x]


# -------------------------
# LIST
# -------------------------
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

    out, seen = [], set()
    for u in links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def collect_job_urls_all_pages(session: requests.Session, max_pages: int = 300) -> List[str]:
    all_urls: List[str] = []
    seen: Set[str] = set()

    for page in range(1, max_pages + 1):
        params = {} if page == 1 else {"page": page}
        soup = get_soup(session, LIST_URL, params=params)
        if soup is None:
            print(f"[STOP] page={page} -> 404/no response")
            break

        urls = parse_list_for_job_links(soup)
        if not urls:
            print(f"[STOP] page={page} -> no links")
            break

        new_cnt = 0
        for u in urls:
            if u not in seen:
                seen.add(u)
                all_urls.append(u)
                new_cnt += 1

        print(f"[PAGE] {page} urls={len(urls)} new={new_cnt} total={len(all_urls)}")

        if new_cnt == 0 and page > 2:
            print(f"[STOP] page={page} -> no new urls")
            break

    return all_urls


# -------------------------
# META / SKILLS
# -------------------------
def extract_meta_from_text(lines: List[str]) -> Dict[str, Optional[str]]:
    text = " ".join(lines)

    m = SALARY_RE.search(text)
    salary = norm(m.group("salary")) if m else None

    ws_found = [ws for ws in WORK_STYLES if re.search(rf"\b{re.escape(ws)}\b", text)]
    work_style = ", ".join(ws_found) if ws_found else None

    employment_type = next((et for et in EMP_TYPES if re.search(rf"\b{re.escape(et)}\b", text)), None)
    work_experience = next((ex for ex in EXPERIENCES if re.search(rf"\b{re.escape(ex)}\b", text)), None)

    location = None
    if "Location" in lines:
        i = lines.index("Location")
        stop = set(META_LABELS + SECTION_LABELS)
        for j in range(i + 1, min(i + 8, len(lines))):
            t = lines[j]
            if t in stop:
                break
            if t and len(t) <= 120:
                location = t
                break

    return {
        "work_style": work_style,
        "salary": salary,
        "work_experience": work_experience,
        "employment_type": employment_type,
        "location": location,
    }


def extract_company_name(lines: List[str]) -> Optional[str]:
    if "Company Name" not in lines:
        return None
    i = lines.index("Company Name")
    if i + 1 < len(lines):
        v = lines[i + 1]
        if v and v not in BAD_SKILL_EXACT:
            return v
    return None


def parse_section_text(lines: List[str], section_name: str) -> Optional[str]:
    if section_name not in lines:
        return None
    i = lines.index(section_name)
    stop = set(META_LABELS + SECTION_LABELS + ["Minimum age", "Maximum age", "Comments"])
    buf = []
    for j in range(i + 1, len(lines)):
        t = lines[j]
        if t in stop:
            break
        if t:
            buf.append(t)
    return norm(" ".join(buf)) if buf else None


def extract_required_skills(lines: List[str]) -> List[str]:
    if "Required Skills" not in lines:
        return []

    i = lines.index("Required Skills")
    stop = set(META_LABELS + SECTION_LABELS + ["Address:", "Phone:", "Email:", "Jobs by specializations", "Comments"])
    out = []

    for j in range(i + 1, min(i + 140, len(lines))):
        t = lines[j]
        if t in stop:
            break
        if not t:
            continue
        if t in BAD_SKILL_EXACT:
            continue
        if t in META_VALUES_TO_EXCLUDE_FROM_SKILLS:
            continue
        if EMAIL_RE.search(t) or URL_RE.search(t):
            continue
        if "Updated:" in t:
            continue
        if "UZS" in t or "USD" in t:
            continue
        if 2 <= len(t) <= 45:
            out.append(t)

    res, seen = [], set()
    for x in out:
        if x not in seen:
            res.append(x)
            seen.add(x)
    return res


def extract_category_chips(lines: List[str]) -> List[str]:
    out = []
    for t in lines:
        if not t:
            continue
        if t in BAD_SKILL_EXACT:
            continue
        if t in META_VALUES_TO_EXCLUDE_FROM_SKILLS:
            continue
        if EMAIL_RE.search(t) or URL_RE.search(t):
            continue
        if "Updated:" in t:
            continue
        if "UZS" in t or "USD" in t:
            continue
        if 3 <= len(t) <= 40:
            out.append(t)

    res, seen = [], set()
    for x in out:
        if x not in seen:
            res.append(x)
            seen.add(x)

    return res[:40]


# -------------------------
# KEYWORD MATCH
# -------------------------
def keyword_match_text(text: str, keyword: str) -> bool:
    t = (text or "").lower()
    kw = (keyword or "").strip().lower()

    if "/" in kw:
        parts = [p.strip().lower() for p in kw.split("/") if p.strip()]
        for p in parts:
            if len(p) <= 2:
                if re.search(rf"\b{re.escape(p)}\b", t):
                    return True
            else:
                if p in t:
                    return True
        return False

    if len(kw) <= 2:
        return bool(re.search(rf"\b{re.escape(kw)}\b", t))

    return kw in t


def matched_keywords(job: Dict, keywords: List[str]) -> List[str]:
    hay = " ".join(
        [
            str(job.get("job_title") or ""),
            str(job.get("skills") or ""),
            str(job.get("description") or ""),
            str(job.get("job_subtitle") or ""),
            str(job.get("company_name") or ""),
        ]
    )
    out, seen = [], set()
    for kw in keywords:
        if keyword_match_text(hay, kw) and kw not in seen:
            out.append(kw)
            seen.add(kw)
    return out


# -------------------------
# DB
# -------------------------
def get_conn(cfg: Dict[str, str]) -> pyodbc.Connection:
    return pyodbc.connect(
        f"Driver={cfg['driver']};"
        f"Server={cfg['server']};"
        f"Database={cfg['db_name']};"
        f"Trusted_Connection={cfg['trusted']};"
    )


def load_existing_ids(cursor: pyodbc.Cursor) -> Set[str]:
    cursor.execute("SELECT job_id FROM it_park")
    return {row[0] for row in cursor.fetchall()}


def clip(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s[:n] if len(s) > n else s


def save_to_db(cursor: pyodbc.Cursor, job: Dict) -> bool:
    sql = """
    INSERT INTO it_park(
        job_id, job_title, location, skills, salary, education, job_type,
        company_name, job_url, source, description, job_subtitle, posted_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor.execute(
            sql,
            (
                job["job_id"],
                clip(job.get("job_title"), 100),
                clip(job.get("location"), 100),
                job.get("skills"),
                job.get("salary"),
                clip(job.get("education"), 100),
                clip(job.get("job_type"), 50),
                clip(job.get("company_name"), 100),
                clip(job.get("job_url"), 200),
                clip(job.get("source"), 20),
                job.get("description"),
                clip(job.get("job_subtitle"), 250),
                job["posted_date"],
            ),
        )
        return True
    except pyodbc.IntegrityError:
        return False


# -------------------------
# DETAIL
# -------------------------
def parse_detail(job_url: str, soup: BeautifulSoup) -> Dict:
    job_id = extract_job_id(job_url) or job_url
    root = soup.select_one("main") or soup.body or soup
    lines = lines_from(root)

    h1 = root.select_one("h1") or soup.select_one("h1")
    job_title = norm(h1.get_text(" ", strip=True)) if h1 else None

    updated = None
    for t in lines:
        if t.startswith("Updated:"):
            updated = norm(t.replace("Updated:", ""))
            break

    company_name = extract_company_name(lines)

    meta = extract_meta_from_text(lines)
    work_style = meta["work_style"]
    salary = meta["salary"]
    work_experience = meta["work_experience"]
    employment_type = meta["employment_type"]
    location = meta["location"]

    # ✅ job_type faqat employment_type
    job_type = employment_type

    # ✅ subtitle faqat meta
    parts = []
    if work_style:
        parts.append(work_style)
    if work_experience:
        parts.append(work_experience)
    if employment_type:
        parts.append(employment_type)
    job_subtitle = " | ".join(parts) if parts else None

    description = parse_section_text(lines, "Description")
    tasks = parse_section_text(lines, "Tasks")
    schedule = parse_section_text(lines, "Schedule")
    add_req = parse_section_text(lines, "Additional requirements")

    posted_date = dt.date.today()
    if updated:
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                posted_date = dt.datetime.strptime(updated, fmt).date()
                break
            except ValueError:
                pass

    skills_list = extract_required_skills(lines)
    if not skills_list:
        skills_list = extract_category_chips(lines)
    skills_final = ", ".join(skills_list) if skills_list else None

    desc_parts = []
    if description:
        desc_parts.append("DESCRIPTION: " + description)
    if tasks:
        desc_parts.append("TASKS: " + tasks)
    if schedule:
        desc_parts.append("SCHEDULE: " + schedule)
    if add_req:
        desc_parts.append("ADDITIONAL REQUIREMENTS: " + add_req)
    full_description = "\n\n".join(desc_parts) if desc_parts else None

    return {
        "job_id": job_id,
        "job_title": job_title,
        "location": location,
        "skills": skills_final,
        "salary": salary,
        "education": None,
        "job_type": job_type,
        "company_name": company_name,
        "job_url": job_url,
        "source": "it-market",
        "description": full_description,
        "job_subtitle": job_subtitle,
        "posted_date": posted_date,
    }


# -------------------------
# RUN
# -------------------------
def run(max_pages: int = 300):
    cfg = load_env()
    keywords = load_keywords("job_list.json")

    session = make_session()
    conn = get_conn(cfg)
    cursor = conn.cursor()

    existing_ids = load_existing_ids(cursor)
    print(f"[DB] existing_ids={len(existing_ids)}")

    urls = collect_job_urls_all_pages(session, max_pages=max_pages)
    print(f"\n[LIST DONE] total_urls={len(urls)}")

    inserted = 0
    duplicates = 0
    matched_cnt = 0
    skipped_before_detail = 0
    failed_details = 0

    for url in urls:
        jid = extract_job_id(url) or url
        if jid in existing_ids:
            duplicates += 1
            skipped_before_detail += 1
            continue

        dsoup = get_soup(session, url)
        if dsoup is None:
            failed_details += 1
            continue

        try:
            job = parse_detail(url, dsoup)
        except Exception as e:
            failed_details += 1
            print(f"[WARN] parse_detail error: {url} -> {e}")
            continue

        hits = matched_keywords(job, keywords)
        if not hits:
            continue

        ok = save_to_db(cursor, job)
        if ok:
            conn.commit()
            inserted += 1
            matched_cnt += 1
            existing_ids.add(jid)
            print(
                f"[SAVE] {jid} | {job.get('job_title')} | kw={hits} | "
                f"company={job.get('company_name')} | job_type={job.get('job_type')} | "
                f"salary={job.get('salary')} | loc={job.get('location')}"
            )
        else:
            duplicates += 1
            existing_ids.add(jid)

        # serverga bosim qilmaslik uchun kichik delay
        time.sleep(0.2)

    cursor.close()
    conn.close()

    print("\nDONE")
    print(
        f"matched={matched_cnt} inserted={inserted} duplicates={duplicates} "
        f"skipped_before_detail={skipped_before_detail} failed_details={failed_details} "
        f"scanned_urls={len(urls)}"
    )


if __name__ == "__main__":
    run(max_pages=300)
