import datetime
import hashlib
import json
import os
import time
from pathlib import Path

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# -------------------- UC DESTRUCTOR BUG FIX (WINERROR 6) --------------------
# Windowsda undetected_chromedriver ba'zan process yopilayotganda __del__ ichida
# yana quit() qilib yuboradi -> WinError 6. Buni butunlay yo'qotamiz.
try:
    if hasattr(uc, "Chrome") and hasattr(uc.Chrome, "__del__"):
        uc.Chrome.__del__ = lambda self: None
except Exception:
    pass

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
COOKIES_PATH = BASE_DIR / "cookies.json"
JOBS_PATH = BASE_DIR / "job_list.json"

_TABLE_READY = False


# ------------------------ HELPERS ------------------------
def job_hash(title, company, location, date):
    raw = f"{title}|{company}|{location}|{date}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def clear_and_type(el, text):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)


def ensure_table_exists(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS glassdoor (
            id SERIAL PRIMARY KEY,
            job_hash CHAR(64) UNIQUE NOT NULL,
            title TEXT,
            company TEXT,
            location TEXT,
            location_sub TEXT,
            title_sub TEXT,
            skills TEXT,
            salary TEXT,
            date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def _env_required(key: str) -> str:
    val = os.getenv(key)
    return val.strip() if val else ""


def safe_quit(driver):
    if not driver:
        return
    try:
        driver.quit()
    except Exception:
        pass


def robust_click(driver, wait: WebDriverWait, xpath: str, retries: int = 2) -> bool:
    last_err = None
    for _ in range(retries):
        try:
            el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            return True
        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException) as e:
            last_err = e
            time.sleep(0.25)
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    if last_err:
        raise last_err
    return False


# ------------------------ FAST DB (ONE CONNECTION) ------------------------
class DB:
    def __init__(self):
        self.conn = None
        self.cur = None

    def open(self):
        global _TABLE_READY

        pg_host = _env_required("DB_HOST")
        pg_port = _env_required("DB_PORT") or "5432"
        pg_db = _env_required("DB_NAME")
        pg_user = _env_required("DB_USER")
        pg_password = _env_required("DB_PASSWORD")

        if not all([pg_host, pg_db, pg_user, pg_password]):
            raise ValueError("Postgres .env variables not fully set: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")

        self.conn = psycopg2.connect(
            host=pg_host,
            port=int(pg_port),
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
        )
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

        if not _TABLE_READY:
            ensure_table_exists(self.cur)
            self.conn.commit()
            _TABLE_READY = True

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None
        self.cur = None

    def save(self, title, company, location, location_sub, title_sub, skills, salary, date):
        h = job_hash(title, company, location, date)

        self.cur.execute(
            """
            INSERT INTO glassdoor (
                job_hash, title, company, location,
                location_sub, title_sub, skills,
                salary, date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_hash) DO NOTHING
            """,
            (h, title, company, location, location_sub, title_sub, skills, salary, date),
        )
        self.conn.commit()

        if self.cur.rowcount == 0:
            print(f"⚠️ Duplicate skipped: {title} @ {company}")
        else:
            print(f"✅ Saved: {title} @ {company}")


# ------------------------ SCRAPER ------------------------
class GlassdoorScraper:
    def __init__(self, job, country, driver, db: DB):
        self.job = job
        self.country = country
        self.driver = driver
        self.db = db

        self.wait = WebDriverWait(driver, 8)
        self.wait1 = WebDriverWait(driver, 2)

    def start_scraping(self):
        self.driver.get("https://www.glassdoor.com/Job")
        time.sleep(0.7)

        job_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'jobTitle')]"))
        )
        loc_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'location')]"))
        )

        clear_and_type(job_input, f'"{self.job}"')
        clear_and_type(loc_input, self.country)
        loc_input.send_keys(Keys.ENTER)
        time.sleep(0.9)

        # sort by latest
        cur = self.driver.current_url
        if "sortBy=" not in cur:
            self.driver.get(cur + "&sortBy=date_desc")
        else:
            self.driver.get(cur.replace("sortBy=relevance", "sortBy=date_desc"))
        time.sleep(0.8)

        self.scroll_and_scrape()

    def scroll_and_scrape(self):
        index = 1
        empty = 0
        last_count = 0

        while True:
            if not self.scrape_card(index):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.0)

                cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
                if len(cards) == last_count:
                    empty += 1
                else:
                    empty = 0
                    last_count = len(cards)

                # 3 emas, 2 qilsak tezroq tugaydi
                if empty >= 2:
                    break
            else:
                index += 1

    def scrape_card(self, i):
        cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
        if i > len(cards):
            return False

        base = f"(//ul[@aria-label='Jobs List']/li)[{i}]"
        try:
            robust_click(self.driver, self.wait, base, retries=2)
        except Exception:
            return False

        # detail panel title chiqishini minimal kutamiz
        try:
            title = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//h1[contains(@id,'job-title')]"))
            ).text
        except Exception:
            title = ""

        try:
            company = self.driver.find_element(
                By.XPATH, "//div[contains(@class,'EmployerProfile_employerNameHeading')]"
            ).text
        except Exception:
            company = ""

        try:
            location = self.driver.find_element(
                By.XPATH, f"{base}//div[contains(@data-test,'emp-location')]"
            ).text
        except Exception:
            location = ""

        try:
            posted = self.driver.find_element(
                By.XPATH, f"{base}//div[contains(@data-test,'job-age')]"
            ).text.lower()
        except Exception:
            posted = ""

        try:
            salary = self.driver.find_element(
                By.XPATH, "//div[contains(@id,'job-salary')]"
            ).text
        except Exception:
            salary = ""

        # skills ba'zida sekin, shuning uchun find_elements bilan tezroq
        try:
            skills_elems = self.driver.find_elements(
                By.XPATH, "//li[contains(@class,'PendingQualification_pendingQualification')]"
            )
            skills = ",".join(x.text for x in skills_elems if x.text.strip())
        except Exception:
            skills = ""

        today = datetime.date.today()
        if "30" in posted:
            date = today - datetime.timedelta(days=30)
        elif "d" in posted:
            n = "".join(c for c in posted if c.isdigit())
            date = today - datetime.timedelta(days=int(n or 0))
        else:
            date = today

        self.db.save(
            title=title,
            company=company,
            location=location,
            location_sub=self.country,
            title_sub=self.job,
            skills=skills,
            salary=salary,
            date=date,
        )
        return True


# ------------------------ MAIN ------------------------
if __name__ == "__main__":
    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    # ✅ driver tezroq ishlashi uchun eager strategy
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.page_load_strategy = "eager"  # ✅ TEZ

    driver = uc.Chrome(options=options)

    db = DB()
    db.open()

    # ✅ cookies faqat 1 marta yuklanadi
    try:
        driver.get("https://www.glassdoor.com")
        time.sleep(0.7)
        if COOKIES_PATH.exists():
            with open(COOKIES_PATH, "r", encoding="utf-8") as f:
                for c in json.load(f):
                    c.pop("sameSite", None)
                    try:
                        driver.add_cookie(c)
                    except Exception:
                        pass
            driver.refresh()
            time.sleep(0.7)

        for job in jobs:
            try:
                GlassdoorScraper(job, "United States", driver=driver, db=db).start_scraping()
            except Exception as e:
                print(f"Scrape error: {e}")

    finally:
        db.close()
        safe_quit(driver)
        driver = None
