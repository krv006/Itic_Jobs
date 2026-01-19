import os
import json
import time
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlencode

import psycopg2
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


load_dotenv()

# ===================== PATHS =====================
BASE_DIR = Path(__file__).resolve().parent
KEYWORDS_PATH = Path(os.getenv("JOBS_PATH", str(BASE_DIR / "jobs_list.json")))
COOKIES_PATH = Path(os.getenv("COOKIES_PATH", str(BASE_DIR / "cookies.json")))
PROFILE_DIR = Path(os.getenv("CHROME_PROFILE_DIR", str(BASE_DIR / "chrome_profile")))

# ===================== SITE =====================
BASE_URL = "https://www.gamesjobsdirect.com"
START_URL = "https://www.gamesjobsdirect.com/search"

# ===================== TUNING =====================
DEFAULT_WAIT = 25
SLEEP = float(os.getenv("SLEEP_BETWEEN_ACTIONS", "1.0"))
MAX_PAGES_PER_KEYWORD = int(os.getenv("MAX_PAGES_PER_KEYWORD", "30"))
HEADLESS = os.getenv("HEADLESS", "false").strip().lower() == "true"


# ===================== HELPERS =====================
def _env_required(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return v


def open_db():
    return psycopg2.connect(
        host=_env_required("DB_HOST"),
        port=int(_env_required("DB_PORT")),
        dbname=_env_required("DB_NAME"),
        user=_env_required("DB_USER"),
        password=_env_required("DB_PASSWORD"),
    )


def ensure_table_exists(cur):
    # Create (agar yo'q bo'lsa)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gamesjobsdirect_jobs (
            id BIGSERIAL PRIMARY KEY,
            job_hash CHAR(64) NOT NULL UNIQUE,

            source TEXT NOT NULL DEFAULT 'gamesjobsdirect',

            title TEXT,
            company TEXT,
            location TEXT,
            salary TEXT,
            job_url TEXT,
            posted_date TEXT,
            description TEXT,

            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )

    # Migration (eski table bo'lsa ham)
    cur.execute("ALTER TABLE gamesjobsdirect_jobs ADD COLUMN IF NOT EXISTS keyword TEXT;")
    cur.execute("ALTER TABLE gamesjobsdirect_jobs ADD COLUMN IF NOT EXISTS salary TEXT;")
    cur.execute("ALTER TABLE gamesjobsdirect_jobs ADD COLUMN IF NOT EXISTS posted_date TEXT;")
    cur.execute("ALTER TABLE gamesjobsdirect_jobs ADD COLUMN IF NOT EXISTS description TEXT;")

    # Indexlar
    cur.execute("CREATE INDEX IF NOT EXISTS idx_gjd_keyword ON gamesjobsdirect_jobs (keyword);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_gjd_company ON gamesjobsdirect_jobs (company);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_gjd_location ON gamesjobsdirect_jobs (location);")


def read_keywords() -> list[str]:
    if not KEYWORDS_PATH.exists():
        raise RuntimeError(f"jobs_list.json topilmadi: {KEYWORDS_PATH}")
    data = json.loads(KEYWORDS_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise RuntimeError("jobs_list.json list bo‘lishi kerak!")
    return [str(x).strip() for x in data if str(x).strip()]


def make_hash(title: str, company: str, location: str, job_url: str):
    raw = f"{title}|{company}|{location}|{job_url}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def detect_blocked(html: str) -> bool:
    low = (html or "").lower()
    return any(
        x in low
        for x in [
            "access denied",
            "forbidden",
            "captcha",
            "cloudflare",
            "verify you are human",
            "attention required",
        ]
    )


def create_driver(headless: bool):
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    # ✅ persistent profile
    options.add_argument(f"--user-data-dir={str(PROFILE_DIR)}")

    # Stable args
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-gpu")

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def save_cookies(driver):
    try:
        cookies = driver.get_cookies()
        COOKIES_PATH.write_text(
            json.dumps(cookies, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[COOKIES] saved -> {COOKIES_PATH}")
    except Exception as e:
        print(f"[COOKIES] save failed: {e}")


def load_cookies(driver):
    if not COOKIES_PATH.exists():
        return False
    try:
        cookies = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
        driver.get(BASE_URL)
        time.sleep(1)

        for c in cookies:
            c.pop("sameSite", None)
            try:
                driver.add_cookie(c)
            except Exception:
                # ba'zi cookie lar seleniumga yoqmasligi mumkin, o'tkazib yuboramiz
                continue

        print(f"[COOKIES] loaded <- {COOKIES_PATH}")
        return True
    except Exception as e:
        print(f"[COOKIES] load failed: {e}")
        return False


def find_search_input(driver):
    wait = WebDriverWait(driver, DEFAULT_WAIT)

    try:
        return wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']"))
        )
    except Exception:
        pass

    for css in ["input[name='q']", "input[name='keyword']", "input[name='search']"]:
        try:
            return wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))
        except Exception:
            continue

    return wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input")))


def listing_links_from_html(html: str):
    """
    Universal: listing HTML ichidan job linklarni yig'adi.
    """
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("a[href]")

    links, seen = [], set()

    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/jobs/" not in href and "/job" not in href:
            continue

        full = urljoin(BASE_URL, href)
        if full in seen:
            continue
        seen.add(full)

        title_guess = a.get_text(" ", strip=True) or None
        links.append({"job_url": full, "title_guess": title_guess})

    return links


def parse_detail_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")

    def pick_text(selectors):
        for sel in selectors:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                return el.get_text(" ", strip=True)
        return None

    title = pick_text(["h1", ".job-title", ".title", "header h1"])
    company = pick_text([".company-name", ".company", "[class*='company']"])
    location = pick_text([".location", "[class*='location']"])
    posted_date = pick_text([".job-posted-date", "[class*='posted']"])
    salary = pick_text([".salary", "[class*='salary']"])

    description = None
    for sel in [".job-description", "[class*='description']", "article", "main"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            txt = el.get_text("\n", strip=True)
            if txt and len(txt) > 80:
                description = txt
                break

    return {
        "title": title,
        "company": company,
        "location": location,
        "posted_date": posted_date,
        "salary": salary,
        "description": description,
    }


def db_insert(cur, row: dict) -> int:
    cur.execute(
        """
        INSERT INTO gamesjobsdirect_jobs
        (job_hash, keyword, title, company, location, salary, job_url, posted_date, description, source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'gamesjobsdirect')
        ON CONFLICT (job_hash) DO NOTHING
        """,
        (
            row["job_hash"],
            row.get("keyword"),
            row.get("title"),
            row.get("company"),
            row.get("location"),
            row.get("salary"),
            row.get("job_url"),
            row.get("posted_date"),
            row.get("description"),
        ),
    )
    return cur.rowcount


def open_search_by_url(driver, keyword: str, page: int):
    qs = urlencode({"q": keyword, "page": page})
    driver.get(f"{START_URL}?{qs}")


def click_next_page(driver) -> bool:
    try:
        btns = driver.find_elements(
            By.XPATH, "//a[contains(., 'Next') or contains(., '›') or contains(., '»')]"
        )
        if btns:
            btns[0].click()
            time.sleep(SLEEP)
            return True
    except Exception:
        pass

    try:
        btns = driver.find_elements(By.CSS_SELECTOR, "a[rel='next']")
        if btns:
            btns[0].click()
            time.sleep(SLEEP)
            return True
    except Exception:
        pass

    return False


def manual_verify_if_blocked(driver):
    """
    CAPTCHA/verify chiqsa — user qo'l bilan yechadi.
    Verify bo'lgach sahifani refresh qilib, 3 marta retry qiladi.
    """
    if not detect_blocked(driver.page_source):
        return True

    print("\n[MANUAL] CAPTCHA/verify chiqdi.")
    print("[MANUAL] Chrome oynasida captcha/verify ni yeching.")
    print("[MANUAL] Verify bo'lgach Continue/Submit bo'lsa bosib yuboring.")
    print("[MANUAL] Keyin shu terminalga qaytib ENTER bosing.")
    input()

    # verify'dan keyin reload qilib ko'ramiz
    try:
        driver.refresh()
        time.sleep(2)
    except Exception:
        pass

    for i in range(1, 4):
        html = driver.page_source
        if not detect_blocked(html):
            save_cookies(driver)
            print("[MANUAL] Verified ✅")
            return True

        print(f"[MANUAL] Hali ham blocked... retry {i}/3 (5s kutyapman)")
        time.sleep(5)
        try:
            driver.refresh()
            time.sleep(2)
        except Exception:
            pass

    # Debug dump
    try:
        (BASE_DIR / "blocked_dump.html").write_text(
            driver.page_source, encoding="utf-8"
        )
        print("[DEBUG] blocked_dump.html saqlandi (games papkaga).")
    except Exception:
        pass

    print("[BLOCKED] Verify tugamadi yoki IP/session baribir blok (VPN/proxy bo‘lishi mumkin).")
    return False


# ===================== MAIN =====================
def main():
    keywords = read_keywords()
    print(f"[KEYWORDS] {len(keywords)} -> {keywords}")

    conn = open_db()
    conn.autocommit = False
    cur = conn.cursor()
    ensure_table_exists(cur)
    conn.commit()

    driver = None
    total_seen = 0
    total_inserted = 0

    try:
        driver = create_driver(headless=HEADLESS)

        # cookies load
        load_cookies(driver)

        driver.get(START_URL)
        time.sleep(SLEEP)

        if not manual_verify_if_blocked(driver):
            return

        for kw in keywords:
            print(f"\n=== KEYWORD: {kw} ===")

            open_search_by_url(driver, kw, 1)
            time.sleep(SLEEP)

            if not manual_verify_if_blocked(driver):
                return

            for page in range(1, MAX_PAGES_PER_KEYWORD + 1):
                html = driver.page_source

                if detect_blocked(html):
                    if not manual_verify_if_blocked(driver):
                        return
                    html = driver.page_source

                links = listing_links_from_html(html)
                if not links:
                    print(f"[STOP] keyword='{kw}' page={page} -> links=0")
                    break

                print(f"[PAGE] {page} links={len(links)}")

                for item in links:
                    job_url = item["job_url"]
                    total_seen += 1

                    try:
                        driver.get(job_url)
                        time.sleep(SLEEP)

                        if detect_blocked(driver.page_source):
                            if not manual_verify_if_blocked(driver):
                                return

                        detail = parse_detail_from_html(driver.page_source)
                    except Exception as e:
                        print(f"[DETAIL-ERR] {job_url} -> {e}")
                        detail = {}

                    title = detail.get("title") or item.get("title_guess") or ""
                    company = detail.get("company") or ""
                    location = detail.get("location") or ""

                    row = {
                        "keyword": kw,
                        "job_url": job_url,
                        "title": title,
                        "company": company,
                        "location": location,
                        "salary": detail.get("salary"),
                        "posted_date": detail.get("posted_date"),
                        "description": detail.get("description"),
                    }
                    row["job_hash"] = make_hash(title, company, location, job_url)

                    try:
                        inserted = db_insert(cur, row)
                        if inserted:
                            total_inserted += 1
                            print(f"  [+] INSERT: {title} | {company} | {location}")
                        else:
                            print(f"  [=] DUP:    {title} | {company} | {location}")
                    except Exception as e:
                        conn.rollback()
                        print(f"[DB-ERR] {job_url} -> {e}")
                        continue

                conn.commit()

                # next page
                if click_next_page(driver):
                    continue

                # fallback URL next page
                open_search_by_url(driver, kw, page + 1)
                time.sleep(SLEEP)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

        # WinError 6 fix: safe quit
        try:
            if driver is not None:
                driver.quit()
        except Exception:
            pass

    print(f"\n[DONE] seen={total_seen} inserted={total_inserted}")


if __name__ == "__main__":
    main()
