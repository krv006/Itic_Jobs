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
    NoSuchElementException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

INDEED_HOME = "https://www.indeed.com/"
DEFAULT_WAIT = 20
COOKIES_PATH = "indeed_cookies.json"

MAX_STR_LEN = 600

# =========================================================
# DRIVER
# =========================================================
def create_driver(headless=False):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)


# =========================================================
# ENV & DATABASE
# =========================================================
def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env faylida {key} topilmadi!")
    return val


def open_db():
    conn = psycopg2.connect(
        host=get_env("DB_HOST"),
        port=get_env("DB_PORT"),
        dbname=get_env("DB_NAME"),
        user=get_env("DB_USER"),
        password=get_env("DB_PASSWORD"),
    )
    conn.autocommit = False

    # Jadvalni yaratish / mavjudligini tekshirish
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS indeed (
            id              SERIAL PRIMARY KEY,
            job_id          VARCHAR(120) NOT NULL,
            source          VARCHAR(50) NOT NULL,
            job_title       VARCHAR(600),
            company_name    VARCHAR(600),
            location        VARCHAR(600),
            salary          VARCHAR(600),
            job_type        VARCHAR(600),
            skills          TEXT,
            education       VARCHAR(600),
            job_url         VARCHAR(1200),
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT ux_indeed_jobid_source UNIQUE (job_id, source)
        );
        """)
        conn.commit()

    print("[DB] Jadval 'indeed' tayyor")
    return conn


def save_to_database(
    conn,
    job_id: str,
    source: str,
    job_title: str,
    company_name: str,
    location: str,
    salary: str,
    job_type: str,
    skills: str,
    education: str,
    job_url: str
) -> bool:
    # Uzunlikni cheklash
    job_title     = str(job_title or "")[:MAX_STR_LEN]
    company_name  = str(company_name or "")[:MAX_STR_LEN]
    location      = str(location or "")[:MAX_STR_LEN]
    salary        = str(salary or "")[:MAX_STR_LEN]
    job_type      = str(job_type or "")[:MAX_STR_LEN]
    skills        = str(skills or "")[:4000]
    education     = str(education or "")[:MAX_STR_LEN]
    job_url       = str(job_url or "")[:1200]

    sql = """
    INSERT INTO indeed (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_id, source) DO NOTHING
    RETURNING id, created_at;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (
                job_id, source, job_title, company_name, location,
                salary, job_type, skills, education, job_url
            ))
            if cur.rowcount > 0:
                row = cur.fetchone()
                print(f"  Saqlandi → ID: {row[0]} | created_at: {row[1]}")
            else:
                print("  (mavjud bo'lgani uchun saqlanmadi)")
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"[DB XATO] {str(e)}")
        print(f"   → job_id: {job_id} | title: {job_title[:70]}...")
        return False


# =========================================================
# YORDAMCHI FUNKSİYALAR
# =========================================================
def safe_click(driver, element):
    if not element:
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        element.click()
        return True
    except:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except:
            return False


def get_text_safe(element):
    return element.text.strip() if element else ""


def first_existing(container, selectors, timeout=8):
    end_time = time.time() + timeout
    while time.time() < end_time:
        for by, selector in selectors:
            try:
                elements = container.find_elements(by, selector)
                if elements:
                    return elements[0]
            except:
                pass
        time.sleep(0.35)
    return None


# =========================================================
# COOKIES
# =========================================================
def save_cookies(driver, path=COOKIES_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(driver.get_cookies(), f)
        print("[Cookies] saqlandi")
    except Exception as e:
        print(f"[Cookies saqlash xatosi] {e}")


def load_cookies(driver, path=COOKIES_PATH):
    if not os.path.exists(path):
        print("[Cookies] fayl topilmadi")
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except:
                pass
        driver.refresh()
        time.sleep(3)
        print("[Cookies] yuklandi")
        return True
    except Exception as e:
        print(f"[Cookies yuklash xatosi] {e}")
        return False


# =========================================================
# LOGIN
# =========================================================
def login_google(driver):
    print("[LOGIN] Google orqali urinish")
    driver.get(INDEED_HOME)
    time.sleep(4)

    if first_existing(driver, [(By.XPATH, "//*[contains(text(),'Sign out')]")], 7):
        print("→ Alla qachon kirilgan")
        return True

    sign_in = first_existing(driver, [
        (By.XPATH, "//a[contains(., 'Sign in')]"),
        (By.CSS_SELECTOR, "a[href*='auth'], a[href*='signin']"),
    ], 15)

    if sign_in:
        safe_click(driver, sign_in)
        time.sleep(3.5)

    google_btn = first_existing(driver, [
        (By.XPATH, "//*[contains(., 'Continue with Google') or contains(., 'Google')]"),
    ], 18)

    if google_btn:
        safe_click(driver, google_btn)
    else:
        cont_as = first_existing(driver, [
            (By.XPATH, "//*[contains(., 'Continue as') or contains(., '@')]"),
        ], 10)
        if cont_as:
            safe_click(driver, cont_as)

    time.sleep(5)

    original_handles = set(driver.window_handles)
    popup_handle = None

    for handle in driver.window_handles:
        if handle not in original_handles:
            popup_handle = handle
            break

    if popup_handle:
        driver.switch_to.window(popup_handle)
        try:
            # Email
            try:
                email_inp = wait(driver, 20).until(
                    EC.visibility_of_element_located((By.NAME, "identifierId"))
                )
                email_inp.send_keys(get_env("EMAIL") + Keys.ENTER)
                time.sleep(3.5)
            except TimeoutException:
                pass

            # Password
            try:
                pwd_inp = wait(driver, 20).until(
                    EC.visibility_of_element_located((By.NAME, "Passwd"))
                )
                pwd_inp.send_keys(get_env("EMAIL_PASSWORD") + Keys.ENTER)
                time.sleep(6)
            except TimeoutException:
                pass
        finally:
            try:
                driver.close()
            except:
                pass

            if original_handles:
                driver.switch_to.window(list(original_handles)[0])
            else:
                driver.get(INDEED_HOME)

    time.sleep(6)
    driver.refresh()
    time.sleep(3)

    if first_existing(driver, [(By.XPATH, "//*[contains(text(),'Sign out')]")], 10):
        print("[LOGIN] Muvaffaqiyatli!")
        return True

    print("[LOGIN] Muvaffaqiyatsiz")
    return False


# =========================================================
# JOB DETAILS — yangilangan selectorlar (2025-2026 holati)
# =========================================================
def read_job_details(driver):
    time.sleep(1.5)

    # Company
    company = get_text_safe(first_existing(driver, [
        (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']"),
        (By.CSS_SELECTOR, ".jobsearch-CompanyInfoWithoutHeaderImage span"),
        (By.CSS_SELECTOR, "div[data-company-name='true'] span"),
    ], timeout=6))

    # Location
    location = get_text_safe(first_existing(driver, [
        (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']"),
        (By.CSS_SELECTOR, ".jobsearch-JobMetadataHeader-item-location"),
        (By.CSS_SELECTOR, "div[data-test='jobLocation']"),
    ], timeout=6))

    # Salary
    salary = get_text_safe(first_existing(driver, [
        (By.CSS_SELECTOR, "[data-testid='jobsearch-JobMetadataHeader-item-text']"),
        (By.CSS_SELECTOR, "div[data-testid*='salary'], span[class*='salary']"),
        (By.CSS_SELECTOR, "[aria-label*='salary'], [aria-label*='Pay']"),
        (By.XPATH, "//*[contains(text(),'$') or contains(text(),'Pay') or contains(text(),'hour') or contains(text(),'year')]"),
    ], timeout=8))

    # Job Type
    job_type = get_text_safe(first_existing(driver, [
        (By.XPATH, "//*[contains(text(),'Job type')]/following-sibling::* | //*[contains(text(),'Job Type')]/following-sibling::*"),
        (By.CSS_SELECTOR, "div.jobsearch-JobMetadataHeader-item"),
        (By.CSS_SELECTOR, "span.jobsearch-JobMetadataHeader-item-text"),
    ], timeout=7))

    # Skills & Education — description dan izlash
    skills = ""
    education = ""

    desc_el = first_existing(driver, [
        (By.ID, "jobDescriptionText"),
        (By.CSS_SELECTOR, ".jobsearch-jobDescriptionText"),
    ], timeout=6)

    if desc_el:
        text = get_text_safe(desc_el).lower()
        # Misollar — o'zingiz kengaytira olasiz
        design_tools = ["adobe", "photoshop", "illustrator", "figma", "after effects", "premiere", "blender", "unity"]
        if any(tool in text for tool in design_tools):
            skills += "Design/Animation tools (Adobe Suite, Figma, etc.), "

        degrees = ["bachelor", "master", "degree", "bsc", "msc", "associate", "diploma"]
        if any(deg in text for deg in degrees):
            education += "Higher education likely required"

    return company, location, salary, job_type, skills.strip(", "), education


def get_job_id(url: str) -> str:
    params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    return params.get("vjk", params.get("jk", ["unknown"]))[0]


# =========================================================
# SCRAPER
# =========================================================
def scrape_keyword(driver, conn, keyword: str, max_pages=10):
    q = urllib.parse.quote_plus(keyword)
    driver.get(f"https://www.indeed.com/jobs?q={q}&sort=date")
    time.sleep(4.5)

    saved_count = 0

    for page in range(1, max_pages + 1):
        print(f"[{keyword.upper()}] sahifa {page}")

        cards = driver.find_elements(By.CSS_SELECTOR, "a.jcs-JobTitle, a[data-jk], h2.jobTitle a")

        for card in cards:
            try:
                title = get_text_safe(card)
                if not title:
                    continue

                safe_click(driver, card)
                time.sleep(1.8)

                job_url = driver.current_url
                job_id = get_job_id(job_url)

                comp, loc, sal, jtype, sk, edu = read_job_details(driver)

                if save_to_database(conn, job_id, "indeed.com", title, comp, loc, sal, jtype, sk, edu, job_url):
                    saved_count += 1
                    print(f"  ✅ {saved_count:3d} | {title[:65]}... | salary: {sal[:40]}")

            except Exception as ex:
                print(f"  card xatosi: {str(ex)[:120]}")

        next_btn = first_existing(driver, [
            (By.CSS_SELECTOR, "a[data-testid='pagination-page-next']"),
        ], 7)

        if not next_btn or not safe_click(driver, next_btn):
            break

        time.sleep(4)

    print(f"[{keyword}] tugadi → jami {saved_count} ta saqlandi")


# =========================================================
# MAIN
# =========================================================
def main():
    driver = None
    conn = None

    try:
        driver = create_driver(headless=False)

        if load_cookies(driver):
            driver.get(INDEED_HOME)
            time.sleep(4)
        else:
            if not login_google(driver):
                print("LOGIN muvaffaqiyatsiz → qo'lda kirib cookie saqlang")
                return
            save_cookies(driver)

        conn = open_db()

        keywords = ["Animation", "Graphic Designer", "Motion Graphics"]

        for kw in keywords:
            scrape_keyword(driver, conn, kw.strip())

    except WebDriverException as wde:
        print(f"[BRAUZER XATOSI] {wde}")
    except Exception as e:
        print(f"\nUMUMIY XATO: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass
        if driver:
            try:
                driver.quit()
            except:
                pass


if __name__ == "__main__":
    main()