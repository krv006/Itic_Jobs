import os
import json
import time
import datetime
import hashlib
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ================== ENV ==================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# ================== PATHS ==================
BASE_DIR = Path(__file__).resolve().parent
JOBS_PATH = BASE_DIR / "job_list.json"
COUNTRIES_PATH = BASE_DIR / "countries.json"
COOKIES_PATH = BASE_DIR / "cookies.json"


# ================== DB ==================
def get_pg_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def create_table_if_not_exists():
    query = """
    CREATE TABLE IF NOT EXISTS glassdoor (
        id SERIAL PRIMARY KEY,
        job_hash CHAR(32) UNIQUE NOT NULL,
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
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def generate_job_hash(title, company, location):
    raw = f"{title}|{company}|{location}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def save_to_database(title, company, location, location_sub, title_sub, skills, salary, date):
    job_hash = generate_job_hash(title, company, location)
    conn = None

    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO glassdoor
            (job_hash, title, company, location, location_sub, title_sub, skills, salary, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_hash) DO NOTHING;
            """,
            (job_hash, title, company, location, location_sub, title_sub, skills, salary, date)
        )

        conn.commit()

        if cur.rowcount == 0:
            print(f"⏭️ Duplicate skipped: {title} @ {company}")
            return False

        print(f"✅ Saved: {title} @ {company}")
        return True

    except Exception as e:
        print(f"❌ DB error: {e}")
        return False

    finally:
        if conn:
            conn.close()


# ================== HELPERS ==================
def clear_and_type(el, text: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)


# ================== SCRAPER ==================
class GlassdoorScraper:
    def __init__(self, job: str, country: str, driver):
        self.job = job
        self.country = country
        self.driver = driver
        self.wait = WebDriverWait(driver, 5)
        self.wait1 = WebDriverWait(driver, 2)

        self.load_cookies()
        self.start_scraping()

    def load_cookies(self):
        self.driver.get("https://www.glassdoor.com")
        time.sleep(2)
        if COOKIES_PATH.exists():
            with open(COOKIES_PATH, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                c.pop("sameSite", None)
                try:
                    self.driver.add_cookie(c)
                except:
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

        job_query = f'"{self.job}"' if " " in self.job else self.job

        clear_and_type(job_input, job_query)
        clear_and_type(loc_input, self.country)
        loc_input.send_keys(Keys.ENTER)
        time.sleep(2)

        url = self.driver.current_url
        self.driver.get(url + ("&sortBy=date_desc" if "?" in url else "?sortBy=date_desc"))
        time.sleep(2)

        self.scroll_and_collect()

    def scroll_and_collect(self):
        index = 1
        while True:
            if not self.scrape_page(index):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
                if index > len(cards):
                    print("✅ Finished scraping")
                    break
            else:
                index += 1

    def scrape_page(self, index: int) -> bool:
        cards = self.driver.find_elements(By.XPATH, "//ul[@aria-label='Jobs List']/li")
        if index > len(cards):
            return False

        base = f"(//ul[@aria-label='Jobs List']/li)[{index}]"

        try:
            el = self.wait.until(EC.element_to_be_clickable((By.XPATH, base)))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            time.sleep(1)
        except:
            return False

        try:
            posted_ago = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, f"{base}//div[contains(@data-test,'job-age')]"))
            ).text
        except:
            posted_ago = ""

        try:
            location = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, f"{base}//div[contains(@data-test,'emp-location')]"))
            ).text
        except:
            location = ""

        try:
            company = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'EmployerProfile_employerNameHeading')]"))
            ).text
        except:
            company = ""

        try:
            title = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//h1[contains(@id,'job-title')]"))
            ).text
        except:
            title = ""

        try:
            salary = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@id,'job-salary')]"))
            ).text
        except:
            salary = ""

        try:
            skills = ",".join([
                x.text for x in self.wait1.until(
                    EC.visibility_of_all_elements_located(
                        (By.XPATH, "//li[contains(@class,'PendingQualification_pendingQualification')]")
                    )
                ) if x.text.strip()
            ])
        except:
            skills = ""

        today = datetime.date.today()
        if "30" in posted_ago:
            date = today - datetime.timedelta(days=30)
        elif "d" in posted_ago.lower():
            d = int("".join(filter(str.isdigit, posted_ago)))
            date = today - datetime.timedelta(days=d)
        else:
            date = today

        save_to_database(title, company, location, self.country, self.job, skills, salary, date)
        return True


# ================== RUNNER ==================
if __name__ == "__main__":
    create_table_if_not_exists()

    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    driver = uc.Chrome(options=options)

    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    with open(COUNTRIES_PATH, "r", encoding="utf-8") as f:
        countries = json.load(f)

    for country in countries:
        for job in jobs:
            print(f"\n=== {job} | {country} ===")
            try:
                GlassdoorScraper(job, country, driver)
            except Exception as e:
                print("❌ Error:", e)

    driver.quit()
