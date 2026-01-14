import os
import time
import json
import urllib.parse

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    NoSuchWindowException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

INDEED_HOME = "https://www.indeed.com/"
DEFAULT_WAIT = 15

# =========================================================
# DRIVER
# =========================================================
def create_driver(headless: bool = False, version_main: int | None = None):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    driver = uc.Chrome(options=options, version_main=version_main)
    driver.set_page_load_timeout(60)
    return driver


def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)


# =========================================================
# ENV
# =========================================================
def _env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} topilmadi!")
    return val


# =========================================================
# DATABASE (POSTGRESQL)
# =========================================================
def open_db():
    conn = psycopg2.connect(
        host=_env_required("PG_HOST"),
        port=_env_required("PG_PORT"),
        dbname=_env_required("PG_DB"),
        user=_env_required("PG_USER"),
        password=_env_required("PG_PASSWORD"),
    )
    conn.autocommit = False
    create_table_if_not_exists(conn)
    return conn


def create_table_if_not_exists(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS indeed (
        id SERIAL PRIMARY KEY,
        job_id VARCHAR(100) NOT NULL,
        source VARCHAR(50) NOT NULL,
        job_title VARCHAR(500),
        company_name VARCHAR(500),
        location VARCHAR(255),
        salary VARCHAR(255),
        job_type VARCHAR(255),
        skills TEXT,
        education VARCHAR(255),
        job_url VARCHAR(1000),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT ux_indeed_jobid_source UNIQUE (job_id, source)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def save_to_database(
    conn,
    job_id,
    job_title,
    location,
    skills,
    salary,
    education,
    job_type,
    company_name,
    job_url,
    source,
):
    sql = """
    INSERT INTO indeed (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (job_id, source) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    job_id,
                    source,
                    job_title,
                    company_name,
                    location,
                    salary,
                    job_type,
                    skills,
                    education,
                    job_url,
                ),
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] {e}")
        return False


# =========================================================
# HELPERS
# =========================================================
def safe_click(driver, element):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        element.click()
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False
    except Exception:
        return False


def get_text_safe(el):
    try:
        return el.text.strip()
    except Exception:
        return ""


def first_existing(driver_or_el, selectors, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        for by, sel in selectors:
            els = driver_or_el.find_elements(by, sel)
            if els:
                return els[0]
        time.sleep(0.2)
    return None


# =========================================================
# GOOGLE LOGIN
# =========================================================
def login_google(driver) -> bool:
    print("[LOGIN] Google")

    sign_in = first_existing(driver, [
        (By.XPATH, "//a[contains(., 'Sign in')]"),
        (By.CSS_SELECTOR, "a[href*='login']"),
    ], timeout=10)

    if not sign_in:
        return False

    safe_click(driver, sign_in)
    time.sleep(2)

    google_btn = first_existing(driver, [
        (By.XPATH, "//button[contains(.,'Google')]"),
    ], timeout=10)

    if not google_btn:
        return False

    safe_click(driver, google_btn)
    time.sleep(2)

    email = _env_required("EMAIL")
    password = _env_required("EMAIL_PASSWORD")

    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])

    email_inp = wait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH, "//input[@type='email']"))
    )
    email_inp.send_keys(email + Keys.ENTER)

    pwd_inp = wait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH, "//input[@type='password']"))
    )
    pwd_inp.send_keys(password + Keys.ENTER)

    time.sleep(5)

    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[0])

    print("[LOGIN] OK")
    return True


# =========================================================
# JOB DETAILS
# =========================================================
def read_job_details(driver):
    time.sleep(0.5)

    company = get_text_safe(first_existing(driver, [
        (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']"),
    ]))

    location = get_text_safe(first_existing(driver, [
        (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']"),
    ]))

    salary = get_text_safe(first_existing(driver, [
        (By.XPATH, "//*[contains(text(),'$')]"),
    ]))

    job_type = get_text_safe(first_existing(driver, [
        (By.XPATH, "//*[contains(text(),'Job type')]"),
    ]))

    skills = get_text_safe(first_existing(driver, [
        (By.XPATH, "//*[contains(text(),'Skills')]"),
    ]))

    education = get_text_safe(first_existing(driver, [
        (By.XPATH, "//*[contains(text(),'Education')]"),
    ]))

    return company, location, salary, job_type, skills, education


def get_job_id_from_url(url: str) -> str:
    if "vjk=" in url:
        return url.split("vjk=")[-1].split("&")[0]
    return url[:100]


# =========================================================
# SCRAPER
# =========================================================
def scrape_keyword(driver, conn, keyword: str, max_pages=20):
    q = urllib.parse.quote_plus(keyword)
    url = f"https://www.indeed.com/jobs?q={q}&sort=date"
    driver.get(url)

    page = 0
    saved = 0

    while page < max_pages:
        page += 1
        print(f"[{keyword}] PAGE {page}")

        cards = driver.find_elements(By.XPATH, "//a[contains(@class,'jcs-JobTitle')]")

        for card in cards:
            try:
                title = get_text_safe(card)
                safe_click(driver, card)
                time.sleep(0.5)

                job_url = driver.current_url
                job_id = get_job_id_from_url(job_url)

                company, location, salary, job_type, skills, education = read_job_details(driver)

                if save_to_database(
                    conn,
                    job_id,
                    title,
                    location,
                    skills,
                    salary,
                    education,
                    job_type,
                    company,
                    job_url,
                    "indeed.com",
                ):
                    saved += 1
                    print(f"  ✅ {saved} | {title}")

            except StaleElementReferenceException:
                continue

        next_btn = first_existing(driver, [
            (By.CSS_SELECTOR, "[aria-label='Next Page']"),
        ], timeout=5)

        if not next_btn or not safe_click(driver, next_btn):
            break

        time.sleep(2)

    print(f"[DONE] {keyword} | saved={saved}")


# =========================================================
# MAIN
# =========================================================
def main():
    driver = create_driver(headless=False)
    driver.get(INDEED_HOME)

    if not login_google(driver):
        print("LOGIN FAILED")
        return

    conn = open_db()

    with open("jobs-list.json", "r", encoding="utf-8") as f:
        keywords = json.load(f)

    try:
        for kw in keywords:
            scrape_keyword(driver, conn, kw)
    finally:
        conn.close()
        driver.quit()


if __name__ == "__main__":
    main()
