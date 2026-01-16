import os
import json
import time
from pathlib import Path
from typing import List, Tuple, Set, Optional

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "job_list.json")))

REMOTEOK_URL = os.getenv("REMOTEOK_URL", "https://remoteok.com/")
SOURCE_NAME = os.getenv("REMOTEOK_SOURCE", "remoteok")

# Scrape settings
HEADLESS = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "40"))          # har keyword uchun scroll limit
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "1.2"))     # scrolldan keyin kutish
NO_NEW_LIMIT = int(os.getenv("NO_NEW_LIMIT", "4"))         # ketma-ket necha marta yangi job kelmasa stop
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "30"))

# ---------------- DB ----------------
def _env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return val


def open_db():
    conn = psycopg2.connect(
        host=_env_required("DB_HOST"),
        port=int(_env_required("DB_PORT")),
        dbname=_env_required("DB_NAME"),
        user=_env_required("DB_USER"),
        password=_env_required("DB_PASSWORD"),
    )
    conn.autocommit = False
    return conn


def ensure_table_exists(cur):
    # ✅ FAQAT sen aytgan schema
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


def insert_rows(conn, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO public.remoteok (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url
    )
    VALUES %s
    ON CONFLICT (job_id, source) DO NOTHING;
    """

    with conn.cursor() as cur:
        ensure_table_exists(cur)
        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        before = cur.fetchone()[0]

        execute_values(cur, sql, rows, page_size=200)

        cur.execute("SELECT COUNT(*) FROM public.remoteok;")
        after = cur.fetchone()[0]

    conn.commit()
    return after - before


# ---------------- INPUT ----------------
def load_keywords() -> List[str]:
    if not JOBS_PATH.exists():
        raise RuntimeError(f"job_list.json topilmadi: {JOBS_PATH}")
    data = json.loads(JOBS_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise RuntimeError('job_list.json LIST bo‘lishi kerak: ["python","react"]')
    return [str(x).strip() for x in data if str(x).strip()]


# ---------------- SELENIUM ----------------
def create_driver() -> uc.Chrome:
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def find_search_input(driver):
    """
    RemoteOK search input selectorlar turlicha bo‘lishi mumkin.
    Shu list bo‘yicha topamiz.
    """
    candidates = [
        (By.CSS_SELECTOR, "input#search"),
        (By.CSS_SELECTOR, "input[name='search']"),
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input[placeholder*='Search' i]"),
        (By.CSS_SELECTOR, "input[placeholder*='Type' i]"),
    ]
    for by, sel in candidates:
        els = driver.find_elements(by, sel)
        if els:
            return els[0]
    raise RuntimeError("Search input topilmadi. RemoteOK UI o‘zgargan bo‘lishi mumkin.")


def clear_and_type(el, text: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)
    el.send_keys(Keys.ENTER)


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def extract_job_rows(driver) -> List[Tuple]:
    """
    RemoteOK list ko‘pincha table rows ko‘rinishida bo‘ladi.
    Asosiy target: job card / row elementlari.
    """
    rows_out: List[Tuple] = []

    # eng ko‘p uchraydigan: tr.job
    job_rows = driver.find_elements(By.CSS_SELECTOR, "tr.job")
    if not job_rows:
        # fallback: job cards bo‘lishi mumkin
        job_rows = driver.find_elements(By.CSS_SELECTOR, "[data-id].job") or driver.find_elements(By.CSS_SELECTOR, "[data-id]")

    for row in job_rows:
        try:
            data_id = row.get_attribute("data-id") or ""
            job_id = f"remoteok_{data_id}" if data_id else None

            # title
            title_el = None
            for sel in ["h2", "a h2", ".company_and_position h2"]:
                els = row.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    title_el = els[0]
                    break
            job_title = safe_text(title_el) if title_el else None

            # company
            company_el = None
            for sel in ["h3", "a h3", ".company_and_position h3"]:
                els = row.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    company_el = els[0]
                    break
            company_name = safe_text(company_el) if company_el else None

            # location
            location = None
            loc_els = row.find_elements(By.CSS_SELECTOR, ".location")
            if loc_els:
                location = safe_text(loc_els[0]) or None

            # salary
            salary = None
            sal_els = row.find_elements(By.CSS_SELECTOR, ".salary")
            if sal_els:
                salary = safe_text(sal_els[0]) or None

            # tags -> skills
            tags = row.find_elements(By.CSS_SELECTOR, ".tags a, .tags span, a.tag")
            skills = ", ".join([safe_text(t) for t in tags if safe_text(t)]) or None

            # job_type (taglardan taxmin)
            job_type = None
            if skills:
                low = skills.lower()
                if "full-time" in low or "full time" in low:
                    job_type = "Full-time"
                elif "part-time" in low or "part time" in low:
                    job_type = "Part-time"
                elif "contract" in low:
                    job_type = "Contract"
                elif "freelance" in low:
                    job_type = "Freelance"
                elif "intern" in low:
                    job_type = "Internship"

            # job_url
            job_url = None
            a_els = row.find_elements(By.CSS_SELECTOR, "a[href*='/remote-jobs/'], a[href*='/l/'], a[href]")
            if a_els:
                href = a_els[0].get_attribute("href")
                if href and href.startswith("http"):
                    job_url = href

            # job_id yo‘q bo‘lsa (ui o‘zgarsa) -> fallback stable id: url
            if not job_id:
                if job_url:
                    job_id = f"remoteok_{job_url.split('/')[-1][:120]}"
                else:
                    # oxirgi fallback: title+company
                    job_id = f"remoteok_{(job_title or 'job')}_{(company_name or 'company')}".replace(" ", "_")[:180]

            rows_out.append(
                (
                    job_id,
                    SOURCE_NAME,
                    job_title,
                    company_name,
                    location,
                    salary,
                    job_type,
                    skills,
                    None,     # education
                    job_url,
                )
            )
        except StaleElementReferenceException:
            continue
        except Exception:
            continue

    return rows_out


def scroll_collect(driver) -> List[Tuple]:
    """
    Infinite scroll: pastga tushgan sari yangi joblar keladi.
    """
    collected: List[Tuple] = []
    seen_ids: Set[str] = set()

    no_new_rounds = 0

    for i in range(MAX_SCROLLS):
        rows = extract_job_rows(driver)

        new_count = 0
        for r in rows:
            job_id = r[0]
            if job_id not in seen_ids:
                seen_ids.add(job_id)
                collected.append(r)
                new_count += 1

        if new_count == 0:
            no_new_rounds += 1
        else:
            no_new_rounds = 0

        print(f"[SCROLL] {i+1}/{MAX_SCROLLS} total_unique={len(collected)} new={new_count}")

        if no_new_rounds >= NO_NEW_LIMIT:
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

    return collected


def main():
    keywords = load_keywords()
    print(f"[KEYWORDS] {len(keywords)} -> {keywords[:10]}{'...' if len(keywords) > 10 else ''}")

    conn = open_db()
    driver = create_driver()

    try:
        driver.get(REMOTEOK_URL)

        # page ready
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        search = find_search_input(driver)

        grand_total = 0
        grand_new = 0

        for kw in keywords:
            print(f"\n=== KEYWORD: {kw} ===")

            # search inputga yozamiz
            clear_and_type(search, kw)

            # natijalar yuklanishini biroz kutamiz
            time.sleep(2.0)

            # yuqoriga qaytib, keyin scroll
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)

            rows = scroll_collect(driver)
            print(f"[COLLECT] keyword='{kw}' rows={len(rows)}")

            new_inserted = insert_rows(conn, rows)
            grand_total += len(rows)
            grand_new += new_inserted

            print(f"[DB] keyword='{kw}' new_inserted={new_inserted}")

            # next keyword uchun tepaga qayt
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1.0)

        print(f"\n[DONE] total_scraped_rows={grand_total} total_new_inserted={grand_new}")

    except TimeoutException as e:
        conn.rollback()
        print("[ERROR] TimeoutException:", repr(e))
        raise
    except Exception as e:
        conn.rollback()
        print("[ERROR]", repr(e))
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
