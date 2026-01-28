import os
import time
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

    driver = uc.Chrome(options=options, version_main=version_main) if version_main else uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)


# ----------------------------
# Env + DB (Postgres)
# ----------------------------
def _env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} topilmadi yoki bo‘sh!")
    return val


def open_db():
    """
    Supports either:
      - DATABASE_URL=postgresql://user:pass@host:port/db
    or
      - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    db_url = os.getenv("DATABASE_URL", "").strip()

    if db_url:
        conn = psycopg2.connect(db_url)
    else:
        host = _env_required("DB_HOST").strip()
        port = _env_required("DB_PORT").strip()
        dbname = _env_required("DB_NAME").strip()
        user = _env_required("DB_USER").strip()
        password = _env_required("DB_PASSWORD").strip()
        conn = psycopg2.connect(
            host=host, port=int(port), dbname=dbname, user=user, password=password
        )

    conn.autocommit = False
    return conn


def ensure_indeed_table(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS indeed (
        id BIGSERIAL PRIMARY KEY,
        job_id VARCHAR(100) NOT NULL,
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
        CONSTRAINT ux_indeed_jobid_source UNIQUE (job_id, source)
    );
    """
    cur = conn.cursor()
    cur.execute(ddl)
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
    # Upsert: DO NOTHING on duplicate (job_id, source)
    sql = """
    INSERT INTO indeed (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_id, source) DO NOTHING;
    """
    try:
        cur = conn.cursor()
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
        # rowcount: 1 => inserted, 0 => already exists
        inserted = cur.rowcount == 1
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] job_id={job_id} -> {e}")
        return False


# ----------------------------
# Selenium helpers
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


def first_existing(driver_or_el, selectors, timeout=5):
    t_end = time.time() + timeout
    while time.time() < t_end:
        for by, sel in selectors:
            els = driver_or_el.find_elements(by, sel)
            if els:
                return els[0]
        time.sleep(0.2)
    return None


def normalize_job_url(href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("/"):
        return urllib.parse.urljoin("https://www.indeed.com", href)
    return href


def get_job_id_from_url(url: str) -> str:
    if "vjk=" in url:
        return url.split("vjk=")[-1].split("&")[0]
    return url.strip()[:100]


# ----------------------------
# Login (Google) - from .env
# ----------------------------
def login_google(driver) -> bool:
    print("Logging into Indeed using Google...")

    try:
        wait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        return False

    sign_in = first_existing(
        driver,
        [
            (By.XPATH, "//a[contains(., 'Sign in') or contains(., 'Sign In')]"),
            (By.CSS_SELECTOR, "a[href*='account/login']"),
        ],
        timeout=10,
    )

    if not sign_in:
        print("Sign in link not found.")
        return False

    safe_click(driver, sign_in)
    time.sleep(2)

    google_btn = first_existing(
        driver,
        [
            (By.ID, "login-google-button"),
            (By.CSS_SELECTOR, "[data-testid='google-login-button']"),
            (By.XPATH, "//button[contains(.,'Google') or contains(.,'Continue with Google')]"),
        ],
        timeout=15,
    )

    if not google_btn:
        print("Google login button not found.")
        return False

    safe_click(driver, google_btn)
    time.sleep(2)

    email = _env_required("EMAIL")
    password = _env_required("EMAIL_PASSWORD")

    try:
        wins = driver.window_handles
        if len(wins) > 1:
            driver.switch_to.window(wins[-1])
    except NoSuchWindowException:
        print("Google login window not available.")
        return False

    # account chooser
    try:
        use_another = driver.find_elements(By.XPATH, "//*[contains(.,'Use another account')]/..")
        if use_another:
            js_click(driver, use_another[0])
            time.sleep(1)
    except Exception:
        pass

    # EMAIL
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

    # PASSWORD
    try:
        pwd_inp = wait(driver, 25).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='password' or @name='Passwd']"))
        )
        try:
            pwd_inp.click()
            pwd_inp.send_keys(password)
        except Exception:
            js_click(driver, pwd_inp)
            pwd_inp.send_keys(password)

        pwd_inp.send_keys(Keys.ENTER)

    except Exception as e:
        try:
            driver.save_screenshot("google_login_password_error.png")
            print("Saved screenshot: google_login_password_error.png")
        except Exception:
            pass

        print(f"Google password step failed: {e}")
        return False

    time.sleep(3)

    try:
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[0])
    except Exception:
        pass

    print("Login successful. Starting job scraping...")
    return True


# ----------------------------
# Job details (right panel)
# ----------------------------
def read_job_details_from_right_panel(driver):
    time.sleep(0.6)

    panel = (
            first_existing(
                driver,
                [
                    (By.ID, "jobsearch-ViewjobPaneWrapper"),
                    (By.CSS_SELECTOR, "div#jobsearch-ViewjobPaneWrapper"),
                    (By.CSS_SELECTOR, "div.jobsearch-RightPane"),
                    (By.CSS_SELECTOR, "div.jobsearch-JobComponent"),
                ],
                timeout=3,
            )
            or driver
    )

    # salary
    salary = ""
    pay_value = first_existing(
        panel,
        [
            (By.XPATH, ".//*[normalize-space()='Pay']/following::*[self::span or self::div][1]"),
            (By.XPATH, ".//*[normalize-space()='Pay']/following::*[contains(@class,'css')][1]"),
        ],
        timeout=2,
    )
    if pay_value:
        salary = get_text_safe(pay_value)

    if not salary:
        header_pay = first_existing(
            panel,
            [
                (By.XPATH, ".//*[contains(.,'$') and contains(.,'a year')]"),
                (By.XPATH, ".//*[contains(.,'$') and contains(.,'an hour')]"),
            ],
            timeout=1,
        )
        if header_pay:
            txt = get_text_safe(header_pay)
            salary = txt.split(" - ")[0].strip()

    # company
    company = ""
    el = first_existing(
        panel,
        [
            (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']"),
            (By.XPATH, ".//*[@data-testid='inlineHeader-companyName']"),
        ],
        timeout=2,
    )
    if el:
        company = get_text_safe(el)

    # location
    location = ""
    el = first_existing(
        panel,
        [
            (By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']"),
            (By.XPATH, ".//*[@data-testid='inlineHeader-companyLocation']"),
        ],
        timeout=2,
    )
    if el:
        location = get_text_safe(el)

    # job type
    job_type = ""
    jt = first_existing(
        panel,
        [
            (By.XPATH, ".//*[normalize-space()='Job type']/following::*[self::span or self::div][1]"),
            (By.XPATH, ".//*[contains(@aria-label,'Job type')]"),
        ],
        timeout=2,
    )
    if jt:
        job_type = get_text_safe(jt).replace("Job type", "").strip()

    # skills
    skills = ""
    btn_more = first_existing(
        panel,
        [(By.XPATH, ".//button[contains(., 'show more') or contains(., '+ show more')]")],
        timeout=1,
    )
    if btn_more:
        safe_click(driver, btn_more)
        time.sleep(0.3)

    sk_el = first_existing(
        panel,
        [
            (By.CSS_SELECTOR, "ul.js-match-insights-provider"),
            (By.XPATH, ".//div[contains(@aria-label,'Skills')]//ul"),
        ],
        timeout=2,
    )
    if sk_el:
        raw = get_text_safe(sk_el)
        raw = (
            raw.replace("Skills", "")
            .replace("+ show more", "")
            .replace("- show less", "")
            .replace("(Required)", "")
            .replace("\n", ",")
        )
        parts = [p.strip() for p in raw.split(",")]
        parts = [p for p in parts if p and "Do you have" not in p]
        skills = ",".join(parts)

    # education
    education = "No Degree Required"
    ed_el = first_existing(
        panel,
        [
            (By.XPATH, ".//*[@aria-label='Education']"),
            (By.XPATH, ".//*[contains(@aria-label,'Education')]"),
        ],
        timeout=2,
    )
    if ed_el:
        raw = get_text_safe(ed_el)
        raw = raw.replace("Education", "").replace("(Required)", "").replace("\n", ",")
        parts = [p.strip() for p in raw.split(",")]
        parts = [p for p in parts if p and "Do you have" not in p]
        if parts:
            education = ",".join(parts)

    return company, location, salary, job_type, skills, education


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

        # filter: only li that actually has a job title link
        job_cards = container.find_elements(By.XPATH, ".//li[.//a[contains(@class,'jcs-JobTitle')]]")
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

                # IMPORTANT FIX: use href for stable job_url/job_id (URL often doesn't change on right-pane click)
                href = normalize_job_url(title_link.get_attribute("href") or "")
                if not href:
                    # fallback, but usually href exists
                    href = driver.current_url

                job_id = get_job_id_from_url(href)

                # click to load right panel details
                if not safe_click(driver, title_link):
                    continue

                time.sleep(0.6)

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
                    job_url=href,
                    source="indeed.com",
                )

                if saved:
                    total_saved += 1
                    print(f"  ✅ saved #{total_saved}: {title} | {company} | {location} | {salary}")
                else:
                    # already exists
                    pass

            except StaleElementReferenceException:
                continue
            except Exception as e:
                print(f"  [CARD ERROR] idx={idx} -> {e}")
                continue

        # pagination: wait for new page after click
        old_first = None
        try:
            old_first = container.find_element(By.CSS_SELECTOR, "a.jcs-JobTitle")
        except Exception:
            pass

        if not click_next_or_stop(driver):
            print("[STOP] Next page yo‘q (oxirgi sahifa yoki pagination yo‘q).")
            break

        if old_first:
            try:
                WebDriverWait(driver, 15).until(EC.staleness_of(old_first))
            except Exception:
                pass

        time.sleep(1.5)

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
    ensure_indeed_table(conn)

    import json
    with open("jobs-list.json", "r", encoding="utf-8") as f:
        keywords = json.load(f)

    try:
        for kw in keywords:
            kw = str(kw).strip()
            if kw:
                scrape_keyword(driver, conn, kw, max_pages=30)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        driver.quit()


if __name__ == "__main__":
    main()
