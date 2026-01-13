import datetime
import hashlib
import json
import os
import time
from pathlib import Path

import pyodbc

import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options


load_dotenv()

# ---------------- PATHS ----------------
BASE_DIR = Path(__file__).resolve().parent
CONN_PATH = BASE_DIR / "conn.json"
COOKIES_PATH = BASE_DIR / "cookies.json"
JOBS_PATH = BASE_DIR / "job_list.json"


# ---------------- HELPERS ----------------
def job_hash(title, company, location, date):
    raw = f"{title}|{company}|{location}|{date}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def clear_and_type(el, text):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)


# ---------------- DATABASE ----------------
def save_to_database(title, company, location, location_sub, title_sub, skills, salary, date):
    conn = None
    try:
        driver = os.getenv("DB_DRIVER")
        server = os.getenv("DB_SERVER")
        db_name = os.getenv("DB_NAME")
        trusted = os.getenv("DB_TRUSTED_CONNECTION", "yes")

        if not all([driver, server, db_name]):
            raise ValueError("DB .env variables not fully set")

        conn = pyodbc.connect(
            f"Driver={driver};"
            f"Server={server};"
            f"Database={db_name};"
            f"Trusted_Connection={trusted};",
            autocommit=False
        )

        cursor = conn.cursor()
        h = job_hash(title, company, location, date)

        cursor.execute(
            """
            INSERT INTO dbo.Glassdoor (
                job_hash, title, company, location,
                location_sub, title_sub, skills,
                salary, [date]
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (h, title, company, location, location_sub, title_sub, skills, salary, date)
        )

        conn.commit()
        print(f"✅ Saved: {title} @ {company}")

    except pyodbc.IntegrityError:
        print(f"⚠️ Duplicate skipped: {title} @ {company}")

    except Exception as e:
        print(f"❌ DB error: {e}")

    finally:
        if conn:
            conn.close()


# ---------------- SCRAPER ----------------
class GlassdoorScraper:
    def __init__(self, job, country, driver=None, headless=False):
        self.job = job
        self.country = country
        self.start = 1

        if driver == None:
            options = uc.ChromeOptions()
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--start-maximized")
            if headless:
                options.add_argument("--headless")
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
        except:
            return False

        try:
            company = self.wait1.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[contains(@class,'EmployerProfile_employerNameHeading')]"))
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
            location = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, f"{base}//div[contains(@data-test,'emp-location')]"))
            ).text
        except:
            location = ""

        try:
            posted = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, f"{base}//div[contains(@data-test,'job-age')]"))
            ).text.lower()
        except:
            posted = ""

        try:
            salary = self.wait1.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@id,'job-salary')]"))
            ).text
        except:
            salary = ""

        try:
            skills = ",".join(x.text for x in self.wait1.until(
                EC.visibility_of_all_elements_located(
                    (By.XPATH, "//li[contains(@class,'PendingQualification_pendingQualification')]")
                )
            ) if x.text.strip())
        except:
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
    #with open(JOBS_PATH, "r", encoding="utf-8") as f:
    #    jobs = json.load(f)
    jobs = ["data analyst"]
    for job in jobs:
        try:
            GlassdoorScraper(job, "United States")
        except Exception as e:
            print(f"Scrape error: {e}")

    
