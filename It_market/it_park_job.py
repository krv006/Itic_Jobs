import datetime as dt
import json
import os
import re
import time
import urllib3
from urllib.parse import urljoin, urlparse

import psycopg2
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from requests.exceptions import SSLError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


# ===========================
# CONFIG
# ===========================
load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://it-market.uz"
LIST_URL = f"{BASE_URL}/job/"

JOB_PATH_RE = re.compile(r"^/job/([A-Za-z0-9]+)/?$")

META_LABELS = [
    "Work style",
    "Salary",
    "Work experience",
    "Employment type",
    "Location",
]

SECTION_LABELS = [
    "Description",
    "Tasks",
    "Schedule",
    "Required Skills",
    "Additional requirements",
]

WORK_STYLES = [
    "Office Work",
    "Remote Work",
    "Partially Remote Work",
]

EMP_TYPES = [
    "Full Time",
    "Part Time",
    "To be discussed",
]

EXPERIENCES = [
    "No experience",
    "From 1 to 3 years",
    "From 3 to 5 years",
    "Over 5 years",
]

SALARY_RE = re.compile(
    r"(?P<from>\d[\d\s]*)\s*(?P<cur>UZS|USD)\s*dan\s*(?P<to>\d[\d\s]*)\s*gacha",
    re.IGNORECASE,
)

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
URL_RE = re.compile(r"(https?://\S+|www\.\S+)", re.I)

BAD_SKILL_EXACT = {
    "IT Market",
    "Companies",
    "Specialists",
    "Jobs",
    "Orders",
    "Contact us",
    "Submit an application",
    "Work style",
    "Salary",
    "Work experience",
    "Employment type",
    "Location",
    "Description",
    "Tasks",
    "Schedule",
    "Required Skills",
    "Additional requirements",
    "Company Name",
    "General information about the employer",
    "Minimum age",
    "Maximum age",
    "Comments",
    "Address:",
    "Phone:",
    "Email:",
    "Jobs by specializations",
}

META_VALUES_TO_EXCLUDE_FROM_SKILLS = set(WORK_STYLES + EMP_TYPES + EXPERIENCES)


# ===========================
# ENV HELPERS
# ===========================
def env_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(key: str, default: int) -> int:
    try:
        return int(str(os.getenv(key, default)).strip())
    except Exception:
        return default


def env_str(key: str, default: str = "") -> str:
    return str(os.getenv(key, default)).strip()


# ===========================
# DATABASE CONNECTION
# ===========================
conn = psycopg2.connect(
    host=env_str("DB_HOST", "localhost"),
    port=env_str("DB_PORT", "5432"),
    dbname=env_str("DB_NAME", "itic"),
    user=env_str("DB_USER", "postgres"),
    password=env_str("DB_PASSWORD", ""),
)

conn.autocommit = True
cursor = conn.cursor()


# ===========================
# SALARY NORMALIZER
# ===========================
def _fmt_num_spaces(n: str) -> str:
    n = re.sub(r"[^\d]", "", n or "")

    if not n:
        return ""

    return f"{int(n):,}".replace(",", " ")


def normalize_itmarket_salary(raw: str) -> str:
    if not raw:
        return ""

    s = re.sub(r"\s+", " ", raw.strip())

    if not s:
        return ""

    m = SALARY_RE.search(s)

    if not m:
        m2 = re.search(
            r"(\d[\d\s]*)\s*(UZS|USD)\s*dan\s*(\d[\d\s]*)\s*gacha",
            s,
            re.IGNORECASE,
        )

        if not m2:
            return s

        frm = _fmt_num_spaces(m2.group(1))
        cur = m2.group(2).upper()
        to = _fmt_num_spaces(m2.group(3))

        if frm and to and cur:
            return f"{frm} - {to} {cur}".strip()

        return s

    frm = _fmt_num_spaces(m.group("from"))
    to = _fmt_num_spaces(m.group("to"))
    cur = (m.group("cur") or "").upper()

    if not frm or not to or not cur:
        return ""

    return f"{frm} - {to} {cur}".strip()


# ===========================
# DB TABLE
# ===========================
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
            search_query TEXT,
            posted_date DATE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source, job_id)
        );
    """)

    cursor.execute("ALTER TABLE it_park ADD COLUMN IF NOT EXISTS search_query TEXT;")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_it_park_source ON it_park (source);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_it_park_posted_date ON it_park (posted_date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_it_park_search_query ON it_park (search_query);")

    print("[DB] it_park table ready ✅")


def save_to_database(data: dict) -> bool:
    cursor.execute("""
        INSERT INTO it_park (
            source,
            job_id,
            job_title,
            location,
            skills,
            salary,
            education,
            job_type,
            company_name,
            job_url,
            description,
            job_subtitle,
            search_query,
            posted_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
        data["search_query"],
        data["posted_date"],
    ))

    return cursor.rowcount == 1


# ===========================
# REQUESTS
# ===========================
def make_session() -> requests.Session:
    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,uz;q=0.6",
        "Connection": "keep-alive",
        "Referer": "https://it-market.uz/",
    })

    return session


def get_soup_requests(
    session: requests.Session,
    url: str,
    params: dict | None = None,
    retries: int = 3,
) -> BeautifulSoup | None:
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            try:
                response = session.get(
                    url,
                    params=params,
                    timeout=(10, 45),
                    allow_redirects=True,
                    verify=True,
                )

            except SSLError as ssl_err:
                last_err = ssl_err
                print(f"[SSL] verify failed, retry verify=False: {url}")

                response = session.get(
                    url,
                    params=params,
                    timeout=(10, 45),
                    allow_redirects=True,
                    verify=False,
                )

            final_url = response.url or ""

            if "it-park.uz" in final_url and "it-market.uz" not in final_url:
                print(f"[WARN] redirected to it-park, ignore requests response: {final_url}")
                return None

            if response.status_code == 404:
                return None

            if response.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {response.status_code}"
                sleep_sec = min(15, 2 ** (attempt - 1))
                print(f"[WARN] {last_err}, retry after {sleep_sec}s: {url}")
                time.sleep(sleep_sec)
                continue

            response.raise_for_status()

            html = response.text or ""

            if len(html.strip()) < 200:
                print(f"[WARN] html too small from requests: {url}")
                return None

            return BeautifulSoup(html, "html.parser")

        except requests.exceptions.RequestException as e:
            last_err = e
            sleep_sec = min(15, 2 ** (attempt - 1))
            print(f"[WARN] requests attempt={attempt} failed: {url} err={e}")
            time.sleep(sleep_sec)

    print(f"[WARN] requests failed finally: {url} err={last_err}")
    return None


# ===========================
# SELENIUM
# ===========================
def create_driver():
    options = Options()

    if env_bool("HEADLESS", "true"):
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors=yes")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-blink-features=AutomationControlled")

    window_size = env_str("CHROME_WINDOW_SIZE", "1920,1080")
    options.add_argument(f"--window-size={window_size}")

    service = Service()
    driver = webdriver.Chrome(service=service, options=options)

    driver.set_page_load_timeout(env_int("PAGE_LOAD_TIMEOUT", 40))
    driver.implicitly_wait(env_int("IMPLICIT_WAIT", 5))

    return driver


def get_soup_selenium(driver, url: str) -> BeautifulSoup | None:
    try:
        driver.get(url)
        time.sleep(env_int("SELENIUM_SLEEP", 2))

        html = driver.page_source or ""

        if len(html.strip()) < 200:
            print(f"[WARN] selenium empty/small page: {url}")
            return None

        return BeautifulSoup(html, "html.parser")

    except Exception as e:
        print(f"[WARN] selenium failed: {url} -> {e}")
        return None


# ===========================
# PARSING HELPERS
# ===========================
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

        if not href:
            continue

        parsed_href = urlparse(href)

        if parsed_href.netloc:
            if "it-market.uz" not in parsed_href.netloc:
                continue

            path = parsed_href.path
        else:
            path = href

        if not path.startswith("/job/"):
            continue

        if "/apply" in path:
            continue

        m = JOB_PATH_RE.match(path.rstrip("/") + "/")

        if not m:
            continue

        full_url = urljoin(BASE_URL, path)
        links.append(full_url)

    result = []
    seen = set()

    for url in links:
        if url not in seen:
            result.append(url)
            seen.add(url)

    return result


def collect_job_urls_all_pages(
    session: requests.Session,
    driver,
    max_pages: int = 300,
) -> list[str]:
    all_urls = []
    seen = set()

    for page in range(1, max_pages + 1):
        page_url = LIST_URL if page == 1 else f"{LIST_URL}?page={page}"

        soup = get_soup_requests(session, page_url)

        if soup is None:
            print(f"[INFO] requests failed for list page={page}, trying Selenium...")
            soup = get_soup_selenium(driver, page_url)

        if soup is None:
            print(f"[STOP] page={page} -> no response / 404")
            break

        urls = parse_list_for_job_links(soup)

        if not urls:
            print(f"[STOP] page={page} -> no links")
            break

        new_count = 0

        for url in urls:
            if url not in seen:
                seen.add(url)
                all_urls.append(url)
                new_count += 1

        print(f"[PAGE] {page} urls={len(urls)} new={new_count} total={len(all_urls)}")

        if new_count == 0 and page > 2:
            print(f"[STOP] page={page} -> no new urls")
            break

        time.sleep(env_int("LIST_PAGE_SLEEP_MS", 500) / 1000)

    return all_urls


def extract_company_name(lines: list[str]) -> str:
    if "Company Name" not in lines:
        return ""

    index = lines.index("Company Name")

    return lines[index + 1] if index + 1 < len(lines) else ""


def parse_section_text(lines: list[str], section_name: str) -> str:
    if section_name not in lines:
        return ""

    index = lines.index(section_name)

    stop_words = set(
        META_LABELS
        + SECTION_LABELS
        + [
            "Minimum age",
            "Maximum age",
            "Comments",
            "Address:",
            "Phone:",
            "Email:",
            "Jobs by specializations",
        ]
    )

    buffer = []

    for j in range(index + 1, len(lines)):
        text = lines[j]

        if text in stop_words:
            break

        if text:
            buffer.append(text)

    return norm(" ".join(buffer))


def extract_meta(lines: list[str]) -> dict:
    text = " ".join(lines)

    salary = ""

    m = SALARY_RE.search(text)

    if m:
        raw_salary = f"{m.group('from')} {m.group('cur')} dan {m.group('to')} gacha"
        salary = normalize_itmarket_salary(raw_salary)

    work_style = ", ".join([ws for ws in WORK_STYLES if ws in text]) or ""
    employment_type = next((et for et in EMP_TYPES if et in text), "")
    work_experience = next((ex for ex in EXPERIENCES if ex in text), "")

    location = ""

    if "Location" in lines:
        index = lines.index("Location")
        stop_words = set(META_LABELS + SECTION_LABELS)

        for j in range(index + 1, min(index + 8, len(lines))):
            item = lines[j]

            if item in stop_words:
                break

            if item and len(item) <= 120:
                location = item
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

    index = lines.index("Required Skills")

    stop_words = set(
        META_LABELS
        + SECTION_LABELS
        + [
            "Address:",
            "Phone:",
            "Email:",
            "Jobs by specializations",
            "Comments",
        ]
    )

    output = []

    for j in range(index + 1, min(index + 150, len(lines))):
        text = lines[j]

        if text in stop_words:
            break

        if not text:
            continue

        if text in BAD_SKILL_EXACT:
            continue

        if text in META_VALUES_TO_EXCLUDE_FROM_SKILLS:
            continue

        if EMAIL_RE.search(text) or URL_RE.search(text):
            continue

        if "Updated:" in text:
            continue

        if "UZS" in text or "USD" in text:
            continue

        if 2 <= len(text) <= 45:
            output.append(text)

    result = []
    seen = set()

    for skill in output:
        if skill not in seen:
            result.append(skill)
            seen.add(skill)

    return result


def extract_category_chips(lines: list[str]) -> list[str]:
    output = []

    for text in lines:
        if not text:
            continue

        if text in BAD_SKILL_EXACT:
            continue

        if text in META_VALUES_TO_EXCLUDE_FROM_SKILLS:
            continue

        if EMAIL_RE.search(text) or URL_RE.search(text):
            continue

        if "Updated:" in text:
            continue

        if "UZS" in text or "USD" in text:
            continue

        if 3 <= len(text) <= 40:
            output.append(text)

    result = []
    seen = set()

    for item in output:
        if item not in seen:
            result.append(item)
            seen.add(item)

    return result[:40]


def parse_posted_date(lines: list[str]) -> dt.date:
    updated = ""

    for text in lines:
        if text.startswith("Updated:"):
            updated = norm(text.replace("Updated:", ""))
            break

    if not updated:
        return dt.date.today()

    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(updated, fmt).date()
        except ValueError:
            pass

    return dt.date.today()


def parse_detail(job_url: str, soup: BeautifulSoup) -> dict:
    job_id = extract_job_id(job_url)

    root = soup.select_one("main") or soup.body or soup
    lines = lines_from(root)

    h1 = soup.select_one("h1")
    job_title = norm(h1.get_text(" ", strip=True)) if h1 else ""

    posted_date = parse_posted_date(lines)

    company_name = extract_company_name(lines)
    meta = extract_meta(lines)

    job_type = meta["employment_type"]

    meta_subtitle = " | ".join([
        item
        for item in [
            meta["work_style"],
            meta["work_experience"],
            meta["employment_type"],
        ]
        if item
    ])

    description = parse_section_text(lines, "Description")
    tasks = parse_section_text(lines, "Tasks")
    schedule = parse_section_text(lines, "Schedule")
    additional_requirements = parse_section_text(lines, "Additional requirements")

    skills_list = extract_required_skills(lines)

    if not skills_list:
        skills_list = extract_category_chips(lines)

    skills_final = ", ".join(skills_list)

    full_description_parts = []

    if description:
        full_description_parts.append("DESCRIPTION: " + description)

    if tasks:
        full_description_parts.append("TASKS: " + tasks)

    if schedule:
        full_description_parts.append("SCHEDULE: " + schedule)

    if additional_requirements:
        full_description_parts.append("ADDITIONAL REQUIREMENTS: " + additional_requirements)

    full_description = "\n\n".join(full_description_parts)

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
        "job_subtitle": meta_subtitle,
        "posted_date": posted_date,
        "search_query": "",
    }


# ===========================
# KEYWORD MATCH
# ===========================
def keyword_match_text(text: str, keyword: str) -> bool:
    source_text = (text or "").lower()
    keyword_text = (keyword or "").strip().lower()

    if not keyword_text:
        return False

    if len(keyword_text) <= 2:
        return bool(re.search(rf"\b{re.escape(keyword_text)}\b", source_text))

    return keyword_text in source_text


def matched_keywords(job: dict, keywords: list[str]) -> list[str]:
    haystack = " ".join([
        job.get("job_title", ""),
        job.get("skills", ""),
        job.get("description", ""),
        job.get("job_subtitle", ""),
        job.get("company_name", ""),
    ])

    return [
        keyword
        for keyword in keywords
        if keyword_match_text(haystack, keyword)
    ]


def pick_primary_query(hits: list[str]) -> str:
    if not hits:
        return ""

    hits_sorted = sorted(hits, key=lambda x: len(str(x)), reverse=True)

    return str(hits_sorted[0]).strip()


# ===========================
# JOB LIST JSON
# ===========================
def load_keywords(path: str = "job_list.json") -> list[str]:
    if not os.path.exists(path):
        print(f"[WARN] {path} not found. Keyword filter disabled.")
        return []

    with open(path, "r", encoding="utf-8") as file:
        keywords = json.load(file)

    if isinstance(keywords, dict):
        for key in ("jobs", "keywords", "list"):
            if key in keywords and isinstance(keywords[key], list):
                keywords = keywords[key]
                break

    if not isinstance(keywords, list):
        print(f"[WARN] {path} format is not list/dict-list. Keyword filter disabled.")
        return []

    result = []

    for item in keywords:
        value = str(item).strip()

        if value:
            result.append(value)

    print(f"[KEYWORDS] loaded={len(result)}")

    return result


# ===========================
# MAIN
# ===========================
def main():
    create_table_if_not_exists()

    keywords = load_keywords("job_list.json")

    session = make_session()
    driver = None

    inserted = 0
    duplicates = 0
    failed_details = 0
    skipped_by_keywords = 0

    try:
        driver = create_driver()

        max_pages = env_int("MAX_PAGES", 300)

        urls = collect_job_urls_all_pages(
            session=session,
            driver=driver,
            max_pages=max_pages,
        )

        print(f"\n[LIST DONE] total_urls={len(urls)}")

        for index, job_url in enumerate(urls, start=1):
            print(f"\n[DETAIL] {index}/{len(urls)} {job_url}")

            soup = get_soup_requests(session, job_url)

            if soup is None or not soup.select_one("h1"):
                print("[INFO] detail requests failed/no h1, trying Selenium...")
                soup = get_soup_selenium(driver, job_url)

            if soup is None:
                failed_details += 1
                print(f"[FAIL] detail no soup: {job_url}")
                continue

            job = parse_detail(job_url, soup)

            if not job.get("job_title"):
                failed_details += 1
                print(f"[FAIL] empty job title: {job_url}")
                continue

            if keywords:
                hits = matched_keywords(job, keywords)

                if not hits:
                    skipped_by_keywords += 1
                    print(f"[SKIP] keyword not matched: {job.get('job_title')}")
                    continue

                job["search_query"] = pick_primary_query(hits)

            else:
                job["search_query"] = ""

            try:
                ok = save_to_database(job)

                if ok:
                    inserted += 1
                    print(
                        f"SAVED: {job['job_id']} | "
                        f"{job.get('job_title')} | "
                        f"salary={job.get('salary')} | "
                        f"search_query={job.get('search_query')}"
                    )
                else:
                    duplicates += 1
                    print(f"DUP: {job['job_id']} | {job.get('job_title')}")

            except Exception as db_err:
                failed_details += 1
                print(f"[DB ERROR] {job_url} -> {db_err}")

            time.sleep(env_int("DETAIL_PAGE_SLEEP_MS", 300) / 1000)

        print("\nDONE ✅")
        print(f"inserted={inserted}")
        print(f"duplicates={duplicates}")
        print(f"failed_details={failed_details}")
        print(f"skipped_by_keywords={skipped_by_keywords}")
        print(f"scanned_urls={len(urls)}")

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