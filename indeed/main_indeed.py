"""
Indeed Scraper (Robust / Production-like)
- undetected_chromedriver
- Google login (robust: visibility/clickable + JS fallback)
- Stable scraping with WebDriverWait
- Pagination safe
- DB insert IF NOT EXISTS (no duplicates)
- Single DB connection for whole run

Requirements:
pip install -U undetected-chromedriver selenium pyodbc

Files:
- conn.json         -> {"driver":"ODBC Driver 17 for SQL Server","server":"...","db_name":"itic"}
- credentials.json  -> {"email":"...","password":"..."}
- jobs-list.json    -> ["python developer","data engineer", ...]
"""

import json
import time
import urllib.parse
import pyodbc
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    NoSuchWindowException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ----------------------------
# Config
# ----------------------------
INDEED_HOME = "https://www.indeed.com/"
DEFAULT_WAIT = 15


# ----------------------------
# Driver
# ----------------------------
def create_driver(headless: bool = False, version_main: int | None = None):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    if version_main:
        driver = uc.Chrome(options=options, version_main=version_main)
    else:
        driver = uc.Chrome(options=options)

    driver.set_page_load_timeout(60)
    return driver


def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)


# ----------------------------
# DB
# ----------------------------
def open_db():
    with open("conn.json", "r", encoding="utf-8") as f:
        conn_dt = json.load(f)

    conn = pyodbc.connect(
        f"Driver={conn_dt['driver']};"
        f"Server={conn_dt['server']};"
        f"Database={conn_dt['db_name']};"
        "Trusted_Connection=yes;"
    )
    conn.autocommit = False
    return conn


def save_to_database(conn, job_id, job_title, location, skills, salary, education, job_type, company_name, job_url,
                     source):
    sql = """
    IF NOT EXISTS (SELECT 1 FROM indeed WHERE job_id = ? AND source = ?)
    BEGIN
        INSERT INTO indeed (
            job_id, job_title, location, skills, salary, education, job_type,
            company_name, job_url, source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    END
    """
    try:
        cur = conn.cursor()
        cur.execute(
            sql,
            job_id, source,
            job_id, job_title, location, skills, salary, education, job_type,
            company_name, job_url, source
        )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] job_id={job_id} -> {e}")
        return False


# ----------------------------
# Helpers
# ----------------------------
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


def js_click(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    driver.execute_script("arguments[0].click();", element)


def get_text_safe(el):
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def first_existing(driver, selectors, timeout=5):
    t_end = time.time() + timeout
    while time.time() < t_end:
        for by, sel in selectors:
            els = driver.find_elements(by, sel)
            if els:
                return els[0]
        time.sleep(0.2)
    return None


# ----------------------------
# Login (Google) - ROBUST
# ----------------------------
def login_google(driver) -> bool:
    print("Logging into Indeed using Google...")

    try:
        wait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        return False

    # Sign in
    sign_in = first_existing(driver, [
        (By.XPATH, "//a[contains(., 'Sign in') or contains(., 'Sign In')]"),
        (By.CSS_SELECTOR, "a[href*='account/login']"),
    ], timeout=10)

    if not sign_in:
        print("Sign in link not found.")
        return False

    safe_click(driver, sign_in)
    time.sleep(2)

    # Google button
    google_btn = first_existing(driver, [
        (By.ID, "login-google-button"),
        (By.CSS_SELECTOR, "[data-testid='google-login-button']"),
        (By.XPATH, "//button[contains(.,'Google') or contains(.,'Continue with Google')]"),
    ], timeout=15)

    if not google_btn:
        print("Google login button not found.")
        return False

    safe_click(driver, google_btn)
    time.sleep(2)

    # creds
    with open("credentials.json", "r", encoding="utf-8") as f:
        creds = json.load(f)
    email = creds["email"]
    password = creds["password"]

    # switch window if opened
    try:
        wins = driver.window_handles
        if len(wins) > 1:
            driver.switch_to.window(wins[-1])
    except NoSuchWindowException:
        print("Google login window not available.")
        return False

    # sometimes account chooser appears
    try:
        use_another = driver.find_elements(By.XPATH, "//*[contains(.,'Use another account')]/..")
        if use_another:
            js_click(driver, use_another[0])
            time.sleep(1)
    except Exception:
        pass

    # EMAIL (VISIBLE)
    try:
        email_inp = wait(driver, 25).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='email' or @name='identifier']"))
        )
        email_inp.clear()
        email_inp.send_keys(email)
        email_inp.send_keys(Keys.ENTER)
    except Exception as e:
        print(f"Google email step failed: {e}")
        return False

    # PASSWORD (VISIBLE + JS CLICK fallback)
    try:
        pwd_inp = wait(driver, 25).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='password' or @name='Passwd']"))
        )

        # interactable bo‘lmasa JS click bilan
        try:
            pwd_inp.click()
            pwd_inp.send_keys(password)
        except Exception:
            js_click(driver, pwd_inp)
            pwd_inp.send_keys(password)

        pwd_inp.send_keys(Keys.ENTER)

    except Exception as e:
        # screenshot for debug
        try:
            driver.save_screenshot("google_login_password_error.png")
            print("Saved screenshot: google_login_password_error.png")
        except Exception:
            pass

        print(f"Google password step failed: {e}")
        return False

    time.sleep(3)

    # back to indeed tab if needed
    try:
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[0])
    except Exception:
        pass

    print("Login successful. Starting job scraping...")
    return True


def read_job_details_from_right_panel(driver):
    time.sleep(0.6)

    # Right panel root (scoped search)
    panel = first_existing(driver, [
        (By.ID, "jobsearch-ViewjobPaneWrapper"),
        (By.CSS_SELECTOR, "div#jobsearch-ViewjobPaneWrapper"),
        (By.CSS_SELECTOR, "div.jobsearch-RightPane"),
        (By.CSS_SELECTOR, "div.jobsearch-JobComponent"),
    ], timeout=3) or driver

    # ---------------- salary (FIX) ----------------
    salary = ""

    # 1) New UI: Job details -> Pay -> value (pill)
    pay_value = first_existing(panel, [
        # Pay labeldan keyingi birinchi qiymat (span/div)
        (By.XPATH, ".//*[normalize-space()='Pay']/following::*[self::span or self::div][1]"),
        # Ba'zan pill ichida bo'ladi
        (By.XPATH, ".//*[normalize-space()='Pay']/following::*[contains(@class,'css')][1]"),
    ], timeout=2)

    if pay_value:
        salary = get_text_safe(pay_value)

    # 2) Fallback: header line "$40,000 a year - Full-time, Contract"
    if not salary:
        header_pay = first_existing(panel, [
            (By.XPATH, ".//*[contains(.,'$') and contains(.,'a year')]"),
            (By.XPATH, ".//*[contains(.,'$') and contains(.,'an hour')]"),
        ], timeout=1)
        if header_pay:
            txt = get_text_safe(header_pay)
            # faqat pay qismini ajratib olish (xohlasang)
            salary = txt.split(" - ")[0].strip()

    # ---------------- company ----------------
    company = ""
    el = first_existing(panel, [
        (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']"),
        (By.XPATH, ".//*[@data-testid='inlineHeader-companyName']"),
    ], timeout=2)
    if el:
        company = get_text_safe(el)

    # ---------------- location ----------------
    location = ""
    el = first_existing(panel, [
        (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']"),
        (By.XPATH, ".//*[@data-testid='inlineHeader-companyLocation']"),
    ], timeout=2)
    if el:
        location = get_text_safe(el)

    # ---------------- job type ----------------
    job_type = ""
    jt = first_existing(panel, [
        (By.XPATH, ".//*[normalize-space()='Job type']/following::*[self::span or self::div][1]"),
        (By.XPATH, ".//*[contains(@aria-label,'Job type')]"),
    ], timeout=2)
    if jt:
        job_type = get_text_safe(jt).replace("Job type", "").strip()

    # ---------------- skills ----------------
    skills = ""
    btn_more = first_existing(panel, [
        (By.XPATH, ".//button[contains(., 'show more') or contains(., '+ show more')]"),
    ], timeout=1)
    if btn_more:
        safe_click(driver, btn_more)
        time.sleep(0.3)

    sk_el = first_existing(panel, [
        (By.CSS_SELECTOR, "ul.js-match-insights-provider"),
        (By.XPATH, ".//div[contains(@aria-label,'Skills')]//ul"),
    ], timeout=2)
    if sk_el:
        raw = get_text_safe(sk_el)
        raw = (raw.replace("Skills", "")
               .replace("+ show more", "")
               .replace("- show less", "")
               .replace("(Required)", "")
               .replace("\n", ","))
        parts = [p.strip() for p in raw.split(",")]
        parts = [p for p in parts if p and "Do you have" not in p]
        skills = ",".join(parts)

    # ---------------- education ----------------
    education = "No Degree Required"
    ed_el = first_existing(panel, [
        (By.XPATH, ".//*[@aria-label='Education']"),
        (By.XPATH, ".//*[contains(@aria-label,'Education')]"),
    ], timeout=2)
    if ed_el:
        raw = get_text_safe(ed_el)
        raw = raw.replace("Education", "").replace("(Required)", "").replace("\n", ",")
        parts = [p.strip() for p in raw.split(",")]
        parts = [p for p in parts if p and "Do you have" not in p]
        if parts:
            education = ",".join(parts)

    return company, location, salary, job_type, skills, education



def get_job_id_from_url(url: str) -> str:
    if "vjk=" in url:
        return url.split("vjk=")[-1].split("&")[0]
    return url.strip()[:100]


# ----------------------------
# Pagination
# ----------------------------
def click_next_or_stop(driver) -> bool:
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='pagination-page-next']"),
        (By.XPATH, "//*[@data-testid='pagination-page-next']"),
        (By.CSS_SELECTOR, "a[aria-label='Next Page']"),
        (By.CSS_SELECTOR, "button[aria-label='Next Page']"),
        (By.XPATH, "//a[contains(@aria-label,'Next')]"),
        (By.XPATH, "//button[contains(@aria-label,'Next')]"),
    ]

    el = first_existing(driver, candidates, timeout=6)
    if not el:
        return False

    aria_disabled = (el.get_attribute("aria-disabled") or "").lower()
    if aria_disabled == "true":
        return False

    cls = (el.get_attribute("class") or "").lower()
    if "disabled" in cls:
        return False

    return safe_click(driver, el)


# ----------------------------
# Scrape one keyword
# ----------------------------
def scrape_keyword(driver, conn, keyword: str, max_pages: int = 30):
    q = urllib.parse.quote_plus(keyword)
    base_url = f"https://www.indeed.com/jobs?q={q}&l=&sort=date&from=searchOnDesktopSerp"
    print(f"\n[KEYWORD] {keyword} -> {base_url}")

    driver.get(base_url)

    try:
        wait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        print("[WARN] Page load timeout.")
        return

    # wait list container
    try:
        wait(driver, 25).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'mosaic-provider-jobcards')]"))
        )
    except TimeoutException:
        print("[WARN] Job list not found (captcha / layout o‘zgargan bo‘lishi mumkin).")
        return

    page = 0
    total_saved = 0

    while page < max_pages:
        page += 1
        print(f"[PAGE] {page}")

        container = driver.find_element(By.XPATH, "//div[contains(@class,'mosaic-provider-jobcards')]")
        job_cards = container.find_elements(By.XPATH, ".//li")  # ✅ .//li (faqat container ichidagi)

        if not job_cards:
            print("[STOP] job cards topilmadi.")
            break

        for idx, card in enumerate(job_cards, start=1):
            try:
                title_link = None
                for sel in [
                    (By.XPATH, ".//a[contains(@class,'jcs-JobTitle')]"),
                    (By.CSS_SELECTOR, "a.jcs-JobTitle"),
                    (By.XPATH, ".//a[contains(@href,'/viewjob')]"),
                ]:
                    els = card.find_elements(*sel)
                    if els:
                        title_link = els[0]
                        break

                if not title_link:
                    continue

                title = get_text_safe(title_link)
                if not title:
                    continue

                if not safe_click(driver, title_link):
                    continue

                time.sleep(0.5)

                current_url = driver.current_url
                job_id = get_job_id_from_url(current_url)

                company, location, salary, job_type, skills, education = read_job_details_from_right_panel(driver)

                saved = save_to_database(
                    conn,
                    job_id=job_id,
                    job_title=title,
                    location=location,
                    skills=skills,
                    salary=salary,
                    education=education,
                    job_type=job_type,
                    company_name=company,
                    job_url=current_url,
                    source="indeed.com",
                )

                if saved:
                    total_saved += 1
                    print(f"  ✅ saved #{total_saved}: {title} | {company} | {location}")

            except StaleElementReferenceException:
                continue
            except Exception as e:
                print(f"  [CARD ERROR] idx={idx} -> {e}")
                continue

        if not click_next_or_stop(driver):
            print("[STOP] Next page yo‘q (oxirgi sahifa yoki pagination yo‘q).")
            break

        time.sleep(2)

    print(f"[DONE] keyword='{keyword}' saved={total_saved}")


# ----------------------------
# Run
# ----------------------------
def main():
    driver = create_driver(headless=False, version_main=None)
    driver.get(INDEED_HOME)

    if not login_google(driver):
        print("Login failed. Exiting.")
        driver.quit()
        return

    conn = open_db()

    with open("jobs-list.json", "r", encoding="utf-8") as f:
        keywords = json.load(f)

    try:
        for kw in keywords:
            kw = str(kw).strip()
            if not kw:
                continue
            scrape_keyword(driver, conn, kw, max_pages=30)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        driver.quit()


if __name__ == "__main__":
    main()
