import datetime as dt
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse

import psycopg2
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

load_dotenv()

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


def env_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(key: str, default: int) -> int:
    try:
        return int(str(os.getenv(key, default)).strip())
    except Exception:
        return default


def env_str(key: str, default: str = "") -> str:
    return str(os.getenv(key, default)).strip()


conn = psycopg2.connect(
    host=env_str("DB_HOST", "localhost"),
    port=env_str("DB_PORT", "5432"),
    dbname=env_str("DB_NAME", "itic"),
    user=env_str("DB_USER", "postgres"),
    password=env_str("DB_PASSWORD", ""),
)
conn.autocommit = True
cursor = conn.cursor()


def create_table_if_not_exists():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS it_park (
            id BIGSERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            job_id TEXT NOT NULL,
            job_title TEXT,
            location TEXT,
            skills TEXT,
            salary TEXT,
            education TEXT,
            job_type TEXT,
            company_name TEXT,
            job_url TEXT,
            description TEXT,
            job_subtitle TEXT,
            posted_date DATE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source, job_id)
        );
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_it_park_source ON it_park (source);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_it_park_posted_date ON it_park (posted_date);")
    print("[DB] it_park table ready ✅")


def save_to_database(data: dict) -> bool:
    cursor.execute("""
        INSERT INTO it_park (
            source, job_id, job_title, location, skills, salary,
            education, job_type, company_name, job_url,
            description, job_subtitle, posted_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (source, job_id) DO NOTHING;
    """, (
        data["source"],
        data["job_id"],
        data["job_title"],
        data["location"],
        data["skills"],
        data["salary"],
        data["education"],
        data["job_type"],
        data["company_name"],
        data["job_url"],
        data["description"],
        data["job_subtitle"],
        data["posted_date"],
    ))
    return cursor.rowcount == 1


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,uz;q=0.7",
        "Connection": "keep-alive",
    })
    return s


def get_soup_requests(session: requests.Session, url: str, params: dict | None = None,
                      retries: int = 3) -> BeautifulSoup | None:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=(10, 45))

            if r.status_code == 404:
                return None

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
                time.sleep(min(15, 2 ** (attempt - 1)))
                continue

            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")

        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(min(15, 2 ** (attempt - 1)))

    print(f"[WARN] requests failed: {url} err={last_err}")
    return None


def create_driver():
    options = Options()

    if env_bool("HEADLESS", "true"):
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    ws = env_str("CHROME_WINDOW_SIZE", "1920,1080")
    options.add_argument(f"--window-size={ws}")

    service = Service()
    driver = webdriver.Chrome(service=service, options=options)

    driver.set_page_load_timeout(env_int("PAGE_LOAD_TIMEOUT", 20))

    driver.implicitly_wait(env_int("IMPLICIT_WAIT", 5))

    return driver


def get_soup_selenium(driver, url: str) -> BeautifulSoup | None:
    try:
        driver.get(url)
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        print(f"[WARN] selenium failed: {url} -> {e}")
        return None


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def extract_job_id(job_url: str) -> str:
    path = urlparse(job_url).path.rstrip("/") + "/"
    m = re.match(r"^/job/([A-Za-z0-9]+)/$", path)
    return m.group(1) if m else job_url


def lines_from(root: Tag) -> list[str]:
    lines = [norm(x) for x in root.get_text("\n").split("\n")]
    return [x for x in lines if x]


def parse_list_for_job_links(soup: BeautifulSoup) -> list[str]:
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


def collect_job_urls_all_pages(session: requests.Session, max_pages: int = 300) -> list[str]:
    all_urls = []
    seen = set()

    for page in range(1, max_pages + 1):
        params = {} if page == 1 else {"page": page}
        soup = get_soup_requests(session, LIST_URL, params=params)

        if soup is None:
            print(f"[STOP] page={page} -> no response / 404")
            break

        urls = parse_list_for_job_links(soup)
        if not urls:
            print(f"[STOP] page={page} -> no links")
            break

        new_count = 0
        for u in urls:
            if u not in seen:
                seen.add(u)
                all_urls.append(u)
                new_count += 1

        print(f"[PAGE] {page} urls={len(urls)} new={new_count} total={len(all_urls)}")

        if new_count == 0 and page > 2:
            print(f"[STOP] page={page} -> no new urls")
            break

        time.sleep(0.2)

    return all_urls


def extract_company_name(lines: list[str]) -> str:
    if "Company Name" not in lines:
        return ""
    i = lines.index("Company Name")
    return lines[i + 1] if i + 1 < len(lines) else ""


def parse_section_text(lines: list[str], section_name: str) -> str:
    if section_name not in lines:
        return ""
    i = lines.index(section_name)
    stop = set(META_LABELS + SECTION_LABELS + ["Minimum age", "Maximum age", "Comments"])
    buf = []
    for j in range(i + 1, len(lines)):
        t = lines[j]
        if t in stop:
            break
        if t:
            buf.append(t)
    return norm(" ".join(buf))


def extract_meta(lines: list[str]) -> dict:
    text = " ".join(lines)

    m = SALARY_RE.search(text)
    salary = norm(m.group("salary")) if m else ""

    work_style = ", ".join([ws for ws in WORK_STYLES if ws in text]) or ""
    employment_type = next((et for et in EMP_TYPES if et in text), "")
    work_experience = next((ex for ex in EXPERIENCES if ex in text), "")

    location = ""
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
        "salary": salary,
        "work_style": work_style,
        "employment_type": employment_type,
        "work_experience": work_experience,
        "location": location,
    }


def extract_required_skills(lines: list[str]) -> list[str]:
    if "Required Skills" not in lines:
        return []

    i = lines.index("Required Skills")
    stop = set(META_LABELS + SECTION_LABELS + ["Address:", "Phone:", "Email:", "Jobs by specializations", "Comments"])
    out = []

    for j in range(i + 1, min(i + 150, len(lines))):
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


def extract_category_chips(lines: list[str]) -> list[str]:
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


def parse_detail(job_url: str, soup: BeautifulSoup) -> dict:
    job_id = extract_job_id(job_url)

    root = soup.select_one("main") or soup.body or soup
    lines = lines_from(root)

    h1 = soup.select_one("h1")
    job_title = norm(h1.get_text(" ", strip=True)) if h1 else ""

    updated = ""
    for t in lines:
        if t.startswith("Updated:"):
            updated = norm(t.replace("Updated:", ""))
            break

    posted_date = dt.date.today()
    if updated:
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                posted_date = dt.datetime.strptime(updated, fmt).date()
                break
            except ValueError:
                pass

    company_name = extract_company_name(lines)
    meta = extract_meta(lines)

    job_type = meta["employment_type"]
    job_subtitle = " | ".join([x for x in [meta["work_style"], meta["work_experience"], meta["employment_type"]] if x])

    description = parse_section_text(lines, "Description")
    tasks = parse_section_text(lines, "Tasks")
    schedule = parse_section_text(lines, "Schedule")
    add_req = parse_section_text(lines, "Additional requirements")

    skills_list = extract_required_skills(lines)
    if not skills_list:
        skills_list = extract_category_chips(lines)
    skills_final = ", ".join(skills_list)

    full_desc_parts = []
    if description:
        full_desc_parts.append("DESCRIPTION: " + description)
    if tasks:
        full_desc_parts.append("TASKS: " + tasks)
    if schedule:
        full_desc_parts.append("SCHEDULE: " + schedule)
    if add_req:
        full_desc_parts.append("ADDITIONAL REQUIREMENTS: " + add_req)

    full_description = "\n\n".join(full_desc_parts)

    return {
        "source": "it-market",
        "job_id": job_id,
        "job_title": job_title,
        "location": meta["location"],
        "skills": skills_final,
        "salary": meta["salary"],
        "education": "",
        "job_type": job_type,
        "company_name": company_name,
        "job_url": job_url,
        "description": full_description,
        "job_subtitle": job_subtitle,
        "posted_date": posted_date,
    }


def keyword_match_text(text: str, keyword: str) -> bool:
    t = (text or "").lower()
    kw = (keyword or "").strip().lower()
    if not kw:
        return False
    if len(kw) <= 2:
        return bool(re.search(rf"\b{re.escape(kw)}\b", t))
    return kw in t


def matched_keywords(job: dict, keywords: list[str]) -> list[str]:
    hay = " ".join([
        job.get("job_title", ""),
        job.get("skills", ""),
        job.get("description", ""),
        job.get("job_subtitle", ""),
        job.get("company_name", ""),
    ])
    return [kw for kw in keywords if keyword_match_text(hay, kw)]


def main():
    create_table_if_not_exists()

    with open("job_list.json", "r", encoding="utf-8") as f:
        keywords = json.load(f)

    session = make_session()
    driver = None

    try:
        driver = create_driver()

        max_pages = env_int("MAX_PAGES", 300)
        urls = collect_job_urls_all_pages(session, max_pages=max_pages)
        print(f"\n[LIST DONE] total_urls={len(urls)}")

        inserted = 0
        duplicates = 0
        failed_details = 0

        for url in urls:
            soup = get_soup_requests(session, url)

            if soup is None or not soup.select_one("h1"):
                soup = get_soup_selenium(driver, url)

            if soup is None:
                failed_details += 1
                continue

            job = parse_detail(url, soup)

            hits = matched_keywords(job, keywords)
            if not hits:
                continue

            ok = save_to_database(job)
            if ok:
                inserted += 1
                print(f"SAVED: {job['job_id']} | {job.get('job_title')} | kw={hits}")
            else:
                duplicates += 1
                print(f"DUP: {job['job_id']}")

            time.sleep(0.2)

        print("\nDONE ✅")
        print(f"inserted={inserted} duplicates={duplicates} failed_details={failed_details} scanned_urls={len(urls)}")

    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
