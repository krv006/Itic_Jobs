import json
import os
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Dict, Any, List

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ===================== CONFIG =====================
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

SOURCE_NAME = "themuse"
BASE_URL = "https://www.themuse.com/search"

DEFAULT_WAIT = int(os.getenv("SELENIUM_WAIT", "25"))
HEADLESS = os.getenv("HEADLESS", "false").strip().lower() in ("1", "true", "yes")
MAX_PAGES = int(os.getenv("MAX_PAGES", "50"))
PAGE_SLEEP = float(os.getenv("PAGE_SLEEP", "0.7"))

# ✅ ENG MUHIM: har jobda DB ga yozamiz
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))
MAX_STALE_RETRY = int(os.getenv("MAX_STALE_RETRY", "3"))


# ===================== DB =====================
def env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return val


def open_db():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn

    conn = psycopg2.connect(
        host=env_required("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=env_required("PG_DB"),
        user=env_required("PG_USER"),
        password=env_required("PG_PASSWORD"),
    )
    conn.autocommit = False
    return conn


def ensure_table_exists(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.themuse (
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
            CONSTRAINT ux_themuse_jobid_source UNIQUE (job_id, source)
        );
        """
    )


def upsert_jobs(cur, rows: List[Dict[str, Any]]):
    if not rows:
        return

    cols = [
        "job_id", "source", "job_title", "company_name", "location",
        "salary", "job_type", "skills", "education", "job_url"
    ]
    values = [tuple(r.get(c) for c in cols) for r in rows]

    sql = f"""
        INSERT INTO public.themuse ({",".join(cols)})
        VALUES %s
        ON CONFLICT (job_id, source) DO NOTHING;
    """
    execute_values(cur, sql, values)


def flush_to_db(conn, cur, batch_rows: List[Dict[str, Any]]):
    if not batch_rows:
        return
    upsert_jobs(cur, batch_rows)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM public.themuse;")
    total = cur.fetchone()[0]
    print(f"[DB] inserted_try={len(batch_rows)} total_rows={total}")
    batch_rows.clear()


# ===================== SELENIUM =====================
def create_driver():
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-gpu")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait(driver, timeout=DEFAULT_WAIT):
    return WebDriverWait(driver, timeout)


def safe_click(driver, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.1)
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False


def close_popups(driver):
    candidates = [
        (By.XPATH, "//button[contains(@aria-label,'Close') or contains(@aria-label,'close')]"),
        (By.XPATH, "//button[contains(.,'Close') or contains(.,'close')]"),
        (By.XPATH, "//button[contains(.,'Accept') or contains(.,'Got it') or contains(.,'I Accept')]"),
    ]
    for by, sel in candidates:
        try:
            for e in driver.find_elements(by, sel)[:2]:
                if e.is_displayed():
                    safe_click(driver, e)
                    time.sleep(0.2)
        except Exception:
            pass


# ===================== PARSING =====================
def build_search_url(keyword: str, page: int) -> str:
    kw = urllib.parse.quote(keyword.strip())
    return f"{BASE_URL}/keyword/{kw}?page={page}"


def parse_job_id_from_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        job_param = qs.get("job", [None])[0]
        if job_param:
            return urllib.parse.unquote(job_param)
    except Exception:
        pass
    return url


def parse_card_location(card_text: str) -> Optional[str]:
    if " - " in card_text:
        loc = card_text.split(" - ", 1)[1].strip()
        loc = re.sub(r"\s+Posted on.*$", "", loc, flags=re.IGNORECASE).strip()
        return loc or None
    return None


def get_view_links(driver):
    return driver.find_elements(By.XPATH, "//a[contains(translate(.,'view job','VIEW JOB'),'VIEW JOB')]")


def click_and_wait_detail(driver, link_el):
    old = ""
    try:
        # mavjud h1
        old = driver.find_element(By.XPATH, "//main//h1").text.strip()
    except Exception:
        pass

    safe_click(driver, link_el)
    time.sleep(0.25)

    def changed(d):
        try:
            t = d.find_element(By.XPATH, "//main//h1").text.strip()
            return bool(t) and t != old
        except Exception:
            return False

    try:
        wait(driver).until(changed)
    except TimeoutException:
        wait(driver).until(EC.presence_of_element_located((By.XPATH, "//main//h1")))


def extract_title(driver) -> Optional[str]:
    # ✅ kuchli selector: main ichidagi h1
    try:
        t = driver.find_element(By.XPATH, "//main//h1").text.strip()
        # "10,000+ jobs" ni title deb olmaslik
        if t and "jobs" not in t.lower():
            return t
    except Exception:
        pass

    # fallback: eng yaqin h1 lar ichidan "jobs" bo‘lmaganini olamiz
    try:
        hs = driver.find_elements(By.XPATH, "//h1")
        for h in hs:
            tx = (h.text or "").strip()
            if tx and "jobs" not in tx.lower():
                return tx
    except Exception:
        pass

    return None


def extract_company(driver) -> Optional[str]:
    # title ustida/ostida company link bo‘ladi
    selectors = [
        "//main//h1/preceding::a[1]",
        "//main//h1/following::a[1]",
    ]
    for sel in selectors:
        try:
            tx = driver.find_element(By.XPATH, sel).text.strip()
            if tx and len(tx) < 200 and "view" not in tx.lower():
                return tx
        except Exception:
            pass
    return None


def extract_right_text(driver) -> str:
    try:
        return driver.find_element(By.XPATH, "//main").text or ""
    except Exception:
        return ""


def extract_salary(text: str) -> Optional[str]:
    m = re.search(r"(\$|£|€)\s?\d[\d,]*(\s?-\s?(\$|£|€)\s?\d[\d,]*)?", text)
    return m.group(0).strip() if m else None


def detect_job_type(text: str) -> Optional[str]:
    t = text.lower()
    if "full-time" in t or "full time" in t:
        return "Full-time"
    if "part-time" in t or "part time" in t:
        return "Part-time"
    if "contract" in t:
        return "Contract"
    if "intern" in t:
        return "Internship"
    if "temporary" in t:
        return "Temporary"
    return None


def detect_education(text: str) -> Optional[str]:
    t = text.lower()
    if "phd" in t or "doctorate" in t:
        return "PhD"
    if "master" in t or "msc" in t:
        return "Master"
    if "bachelor" in t or "undergraduate degree" in t:
        return "Bachelor"
    if "degree" in t:
        return "Degree required"
    return None


def extract_skills(text: str) -> Optional[str]:
    skills = [
        "python", "java", "javascript", "typescript", "react", "react native",
        "node", "django", "flask", "fastapi", "sql", "postgres", "mysql",
        "mongodb", "redis", "aws", "azure", "gcp", "docker", "kubernetes",
    ]
    t = " " + re.sub(r"\s+", " ", text.lower()) + " "
    found = sorted({s for s in skills if f" {s} " in t})
    return ", ".join(found) if found else None


# ===================== SCRAPER =====================
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        raise RuntimeError(f"job_list.json topilmadi: {JOBS_PATH}")

    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return [str(x).strip() for x in data if str(x).strip()]

    for key in ("keywords", "jobs", "job_titles"):
        if key in data and isinstance(data[key], list):
            return [str(x).strip() for x in data[key] if str(x).strip()]

    raise RuntimeError("job_list.json format topilmadi. List yoki {keywords:[...]} bo‘lsin.")


def scrape_keyword(driver, keyword: str, conn):
    cur = conn.cursor()
    ensure_table_exists(cur)
    conn.commit()

    print(f"\n=== KEYWORD: {keyword} ===")

    for page in range(1, MAX_PAGES + 1):
        url = build_search_url(keyword, page)
        print(f"[OPEN] {url}")
        driver.get(url)
        time.sleep(PAGE_SLEEP)
        close_popups(driver)

        try:
            wait(driver).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//a[contains(translate(.,'view job','VIEW JOB'),'VIEW JOB')]")
                )
            )
        except TimeoutException:
            print(f"[STOP] No results or blocked. keyword='{keyword}' page={page}")
            break

        batch_rows: List[Dict[str, Any]] = []
        seen = set()

        i = 0
        while True:
            close_popups(driver)
            links = get_view_links(driver)
            if i >= len(links):
                break

            stale_retry = 0
            while stale_retry < MAX_STALE_RETRY:
                try:
                    link = links[i]

                    # chap card text (location)
                    card_text = ""
                    try:
                        card = link.find_element(By.XPATH, "ancestor::div[1]/ancestor::div[1]")
                        card_text = card.text.strip()
                    except Exception:
                        pass

                    click_and_wait_detail(driver, link)
                    time.sleep(0.2)
                    close_popups(driver)

                    job_url = driver.current_url
                    job_id = parse_job_id_from_url(job_url)

                    if job_id in seen:
                        break
                    seen.add(job_id)

                    detail_text = extract_right_text(driver)

                    row = {
                        "job_id": job_id,
                        "source": SOURCE_NAME,
                        "job_title": extract_title(driver),
                        "company_name": extract_company(driver),
                        "location": parse_card_location(card_text),
                        "salary": extract_salary(detail_text),
                        "job_type": detect_job_type(detail_text),
                        "skills": extract_skills(detail_text),
                        "education": detect_education(detail_text),
                        "job_url": job_url,
                    }

                    batch_rows.append(row)
                    print(f"  [JOB] {row['job_title']} | {row['company_name']} | {row['location']} | id={job_id}")

                    # ✅ HAR JOBDA DB GA YOZADI (BATCH_SIZE=1)
                    if len(batch_rows) >= BATCH_SIZE:
                        flush_to_db(conn, cur, batch_rows)

                    time.sleep(PAGE_SLEEP)
                    break

                except StaleElementReferenceException:
                    stale_retry += 1
                    time.sleep(0.2)
                    continue
                except Exception as e:
                    print("  [ERR]", repr(e))
                    break

            # ✅ eng muhim: i albatta oshishi kerak (qotib qolmasin)
            i += 1

        # page tugadi, qolgan bo‘lsa yozamiz
        try:
            flush_to_db(conn, cur, batch_rows)
        except Exception as e:
            conn.rollback()
            print("[DB ERR]", repr(e))

        print(f"[PAGE DONE] page={page} items_seen={len(seen)}")
        if len(seen) == 0:
            print(f"[DONE] no jobs page={page}")
            break


def main():
    keywords = load_keywords()
    conn = open_db()
    print("[DB] connected:", conn.get_dsn_parameters())

    driver = create_driver()
    try:
        for kw in keywords:
            if kw.strip():
                scrape_keyword(driver, kw.strip(), conn)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
