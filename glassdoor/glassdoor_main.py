import datetime
import hashlib
import json
import os
import time
from pathlib import Path

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
CONN_PATH = BASE_DIR / "conn.json"
COOKIES_PATH = BASE_DIR / "cookies.json"
JOBS_PATH = BASE_DIR / "job_list.json"

# Table create bo'lganini 1 marta belgilab qo'yamiz
_TABLE_READY = False


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
    if val is None:
        return ""
    return val.strip()


def save_to_database(title, company, location, location_sub, title_sub, skills, salary, date):
    global _TABLE_READY
    conn = None
    try:
        # ✅ Senda DB_ nomlar bilan bo'lgani uchun shuni ishlatamiz
        pg_host = _env_required("DB_HOST")
        pg_port = _env_required("DB_PORT") or "5432"
        pg_db = _env_required("DB_NAME")
        pg_user = _env_required("DB_USER")
        pg_password = _env_required("DB_PASSWORD")  # ✅ bo'sh joylarsiz

        if not all([pg_host, pg_db, pg_user, pg_password]):
            raise ValueError(
                "Postgres .env variables not fully set: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD"
            )

        conn = psycopg2.connect(
            host=pg_host,
            port=int(pg_port),
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
        )
        conn.autocommit = False
        cur = conn.cursor()

        # ✅ table create faqat 1 marta (process davomida)
        if not _TABLE_READY:
            ensure_table_exists(cur)
            conn.commit()
            _TABLE_READY = True

        h = job_hash(title, company, location, date)

        # ✅ insert (duplicate -> skip)
        cur.execute(
            """
            INSERT INTO glassdoor (
                job_hash, title, company, location,
                location_sub, title_sub, skills,
                salary, date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_hash) DO NOTHING
            """,
            (h, title, company, location, location_sub, title_sub, skills, salary, date)
        )

        conn.commit()

        if cur.rowcount == 0:
            print(f"⚠️ Duplicate skipped: {title} @ {company}")
        else:
            print(f"✅ Saved: {title} @ {company}")

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"❌ DB error: {e}")

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


class GlassdoorScraper:
    def __init__(self, job, country, driver=None, headless=False):
        self.job = job
        self.country = country
        self.start = 1

        if driver is None:
            options = uc.ChromeOptions()
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--start-maximized")
            if headless:
                options.add_argument("--headless=new")
            driver = uc.Chrome(options=options)

        self.driver = driver
        self.wait = WebDriverWait(driver, 6)
        self.wait1 = WebDriverWait(driver, 2)

        self.load_cookies()
        self.start_scraping()

    def load_cookies(self):
        self.driver.get("https://www.glassdoor.com")
        time.sleep(2)
        if COOKIES_PATH.exists():
            with open(COOKIES_PATH, "r", encoding="utf-8") as f:
                for c in json.load(f):
                    c.pop("sameSite", None)
                    try:
                        self.driver.add_cookie(c)
                    except Exception:
                        pass
            self.driver.refresh()
            time.sleep(2)

    def start_scraping(self):
        self.driver.get("https://www.glassdoor.com/Job")
        time.sleep(2)

        job_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'jobTitle')]"))
        )
        loc_input = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, "//input[contains(@aria-labelledby,'location')]"))
        )

        clear_and_type(job_input, f'"{self.job}"')
        clear_and_type(loc_input, self.country)
        loc_input.send_keys(Keys.ENTER)
        time.sleep(2)

        self.driver.get(self.driver.current_url + "&sortBy=date_desc")
        time.sleep(2)

        self.scroll_and_scrape()

    def scroll_and_scrape(self):
        index = 1
        empty = 0
        last_count = 0

        while True:
            if not self.scrape_card(index):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
                if len(cards) == last_count:
                    empty += 1
                else:
                    empty = 0
                    last_count = len(cards)

                if empty >= 3:
                    break
            else:
                index += 1

    def scrape_card(self, i):
        cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
        if i > len(cards):
            return False

        base = f"(//ul[@aria-label='Jobs List']/li)[{i}]"
        try:
            el = self.wait.until(EC.element_to_be_clickable((By.XPATH, base)))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            time.sleep(1)
        except Exception:
            return False

        try:
            company = self.wait1.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[contains(@class,'EmployerProfile_employerNameHeading')]")
                )
            ).text
        except Exception:
            company = ""

        try:
            title = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//h1[contains(@id,'job-title')]"))
            ).text
        except Exception:
            title = ""

        try:
            location = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, f"{base}//div[contains(@data-test,'emp-location')]"))
            ).text
        except Exception:
            location = ""

        try:
            posted = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, f"{base}//div[contains(@data-test,'job-age')]"))
            ).text.lower()
        except Exception:
            posted = ""

        try:
            salary = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@id,'job-salary')]"))
            ).text
        except Exception:
            salary = ""

        try:
            skills = ",".join(
                x.text
                for x in self.wait1.until(
                    EC.visibility_of_all_elements_located(
                        (By.XPATH, "//li[contains(@class,'PendingQualification_pendingQualification')]")
                    )
                )
                if x.text.strip()
            )
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

        save_to_database(
            title, company, location,
            self.country, self.job,
            skills, salary, date
        )
        return True


if __name__ == "__main__":
    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    driver = uc.Chrome()

    try:
        for job in jobs:
            try:
                GlassdoorScraper(job, "United States", driver=driver)
            except Exception as e:
                print(f"Scrape error: {e}")
    finally:
        # ✅ WinError 6 chiqmasligi uchun "safe quit"
        try:
            driver.quit()
        except Exception:
            pass
