import os
import time
import urllib.parse
import json
import traceback

import psycopg2
from psycopg2 import Error
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    NoSuchWindowException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

INDEED_HOME = "https://www.indeed.com/"
DEFAULT_WAIT = 15

# Country kodlari mapping
COUNTRY_CODE_MAP = {
    "UK": "GB",
    "Japan": "JP",
    "Germany": "DE",
    "Poland": "PL",
    "France": "FR",
    "Switzerland": "CH",
    "London": "GB",
    "Philippines": "PH",
    "United States": "US",
    "China": "CN",
    "Dubai": "AE",
    "Abu Dhabi": "AE",
    "Uzbekistan": "UZ",
    "Kazakhstan": "KZ"
}

# ----------------------------
# Driver
# ----------------------------
def create_driver(headless: bool = False):
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

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)

# ----------------------------
# Env + DB
# ----------------------------
def _env_required(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f".env da {key} topilmadi!")
    return val

def open_db():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        conn = psycopg2.connect(db_url)
    else:
        host = _env_required("DB_HOST")
        port = _env_required("DB_PORT")
        dbname = _env_required("DB_NAME")
        user = _env_required("DB_USER")
        password = _env_required("DB_PASSWORD")
        conn = psycopg2.connect(
            host=host, port=int(port), dbname=dbname, user=user, password=password
        )
    conn.autocommit = False
    return conn

def ensure_indeed_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT to_regclass('public.indeed');")
        result = cur.fetchone()
        if result[0] is None:
            create_sql = """
            CREATE TABLE indeed (
                id BIGSERIAL PRIMARY KEY,
                job_id VARCHAR(100) NOT NULL,
                source VARCHAR(50) NOT NULL,
                job_title TEXT,
                company_name TEXT,
                location TEXT,
                salary TEXT,
                job_type TEXT,
                skills TEXT,
                education TEXT,
                job_url TEXT,
                country TEXT,
                country_code VARCHAR(2),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ux_indeed_jobid_source UNIQUE (job_id, source)
            );
            """
            cur.execute(create_sql)
            conn.commit()
            print("Jadval 'indeed' yangi yaratildi.")
        else:
            print("Jadval 'indeed' allaqachon mavjud.")
    except Error as e:
        conn.rollback()
        print(f"Jadval tekshirish/yaratishda xato: {e}")
        traceback.print_exc()
    finally:
        cur.close()

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
    country,
    country_code,
    source="indeed.com",
):
    sql = """
    INSERT INTO indeed (
        job_id, source, job_title, company_name, location,
        salary, job_type, skills, education, job_url, country, country_code
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_id, source) DO NOTHING;
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, (
            job_id, source, job_title, company_name, location,
            salary, job_type, skills, education, job_url, country, country_code
        ))
        inserted = cur.rowcount == 1
        conn.commit()
        if inserted:
            print(f"  ✅ Saqlandi: {job_title[:60]} | {country} ({country_code})")
        return inserted
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] {job_id} → {e}")
        return False

# ----------------------------
# Selenium helpers
# ----------------------------
def safe_click(driver, element):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        element.click()
        return True
    except:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except:
            return False

def get_text_safe(el):
    try:
        return (el.text or "").strip()
    except:
        return ""

def first_existing(driver_or_el, selectors, timeout=5):
    t_end = time.time() + timeout
    while time.time() < t_end:
        for by, sel in selectors:
            try:
                els = driver_or_el.find_elements(by, sel)
                if els:
                    return els[0]
            except:
                pass
        time.sleep(0.2)
    return None

def normalize_job_url(href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("/"):
        return "https://www.indeed.com" + href
    return href

def get_job_id_from_url(url: str) -> str:
    if "vjk=" in url:
        return url.split("vjk=")[-1].split("&")[0]
    if "jk=" in url:
        return url.split("jk=")[-1].split("&")[0]
    return ""

# ----------------------------
# Login Google — TO‘G‘RILANGAN VA MUSTAHKAMLANGAN
# ----------------------------
def login_google(driver) -> bool:
    print("Indeed ga Google orqali kirish...")

    try:
        # Sahifa yuklanishini kutish
        wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        print("Sahifa yuklanmadi (timeout). Internet yoki brauzer muammosi bo'lishi mumkin.")
        return False

    try:
        # Sign in tugmasini topish
        sign_in = wait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Sign in') or contains(., 'Sign In') or contains(., 'Log in')]"))
        )
        safe_click(driver, sign_in)
        time.sleep(3)
    except TimeoutException:
        print("Sign in tugmasi topilmadi. Sahifani tekshiring.")
        return False
    except Exception as e:
        print(f"Sign in tugmasini bosishda xato: {e}")
        traceback.print_exc()
        return False

    try:
        # Google tugmasi
        google_btn = wait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "login-google-button"))
        )
        safe_click(driver, google_btn)
        time.sleep(4)
    except TimeoutException:
        print("Google tugmasi topilmadi.")
        return False
    except Exception as e:
        print(f"Google tugmasini bosishda xato: {e}")
        traceback.print_exc()
        return False

    # Yangi oyna ochilishini kutish va o'tish
    try:
        wait(driver, 15).until(lambda d: len(d.window_handles) > 1)
        driver.switch_to.window(driver.window_handles[-1])
        print("Google login oynasiga o'tildi.")
    except TimeoutException:
        print("Google login oynasi ochilmadi.")
        return False
    except Exception as e:
        print(f"Oyna o'tkazishda xato: {e}")
        return False

    email = _env_required("EMAIL")
    password = _env_required("EMAIL_PASSWORD")

    try:
        # Email kiritish
        email_inp = wait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='email' or @name='identifier']"))
        )
        email_inp.clear()
        email_inp.send_keys(email)
        email_inp.send_keys(Keys.ENTER)
        time.sleep(4)

        # Parol kiritish
        pwd_inp = wait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='password']"))
        )
        pwd_inp.clear()
        pwd_inp.send_keys(password)
        pwd_inp.send_keys(Keys.ENTER)
        time.sleep(8)

        # Asosiy oynaga qaytish
        driver.switch_to.window(driver.window_handles[0])
        time.sleep(5)
        print("Login muvaffaqiyatli yakunlandi.")
        return True
    except TimeoutException as te:
        print("Login sahifasi elementlari topilmadi (email/parol maydonlari).")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"Login jarayonida jiddiy xato: {e}")
        traceback.print_exc()
        return False

# ----------------------------
# Job details (oldin to'g'rilangan)
# ----------------------------
def read_job_details_from_right_panel(driver):
    time.sleep(1)

    panel = driver
    for sel in ["#jobsearch-ViewjobPaneWrapper", "div.jobsearch-RightPane", "div.jobsearch-JobComponent"]:
        try:
            panel = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except:
            pass

    salary = ""
    try:
        pay_el = first_existing(
            panel,
            [
                (By.XPATH, ".//*[contains(@aria-label, 'Pay')]"),
                (By.XPATH, ".//*[contains(., '$') and (contains(., 'year') or contains(., 'hour'))]"),
            ],
            timeout=3
        )
        if pay_el:
            salary = get_text_safe(pay_el).replace("Pay", "").replace("Employer est.", "").strip()
    except:
        pass

    company = ""
    try:
        company_el = first_existing(
            panel,
            [(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']")],
            timeout=2
        )
        if company_el:
            company = get_text_safe(company_el)
    except:
        pass

    location = ""
    try:
        loc_el = first_existing(
            panel,
            [(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']")],
            timeout=2
        )
        if loc_el:
            location = get_text_safe(loc_el)
    except:
        pass

    job_type = ""
    try:
        jt_el = first_existing(
            panel,
            [(By.XPATH, ".//*[contains(@aria-label, 'Job type')]")],
            timeout=2
        )
        if jt_el:
            raw = get_text_safe(jt_el)
            job_type = raw.replace("Job type", "").strip()
    except:
        pass

    skills = ""
    try:
        more_btn = first_existing(
            panel,
            [(By.XPATH, ".//button[contains(., 'show more') or contains(., '+ show more')]")],
            timeout=1
        )
        if more_btn:
            safe_click(driver, more_btn)
            time.sleep(0.5)

        sk_el = first_existing(
            panel,
            [(By.CSS_SELECTOR, "[aria-label*='Skills'] ul, ul.js-match-insights-provider")],
            timeout=2
        )
        if sk_el:
            raw = get_text_safe(sk_el)
            raw = raw.replace("Skills", "").replace("+ show more", "").replace("- show less", "").replace("(Required)", "")
            parts = [p.strip() for p in raw.split("\n") if p.strip() and "Do you have" not in p]
            skills = ", ".join(parts)
    except:
        pass

    education = "No Degree Required"
    try:
        ed_el = first_existing(
            panel,
            [(By.CSS_SELECTOR, "[aria-label*='Education']")],
            timeout=2
        )
        if ed_el:
            raw = get_text_safe(ed_el).replace("Education", "").replace("(Required)", "")
            parts = [p.strip() for p in raw.split("\n") if p.strip() and "Do you have" not in p]
            if parts:
                education = ", ".join(parts)
    except:
        pass

    return company, location, salary, job_type, skills, education

# ----------------------------
# Pagination
# ----------------------------
def click_next_or_stop(driver) -> bool:
    selectors = [
        (By.CSS_SELECTOR, "[data-testid='pagination-page-next']"),
        (By.CSS_SELECTOR, "a[aria-label*='Next']"),
        (By.XPATH, "//a[contains(@aria-label,'Next')]"),
    ]

    el = None
    for by, sel in selectors:
        try:
            el = wait(driver, 8).until(EC.element_to_be_clickable((by, sel)))
            break
        except:
            pass

    if not el:
        return False

    return safe_click(driver, el)

# ----------------------------
# Scrape keyword + country
# ----------------------------
def scrape_keyword_country(driver, conn, keyword: str, country_name: str, country_code: str = "", max_pages: int = 5):
    q = urllib.parse.quote_plus(keyword)
    l = urllib.parse.quote_plus(country_name)
    base_url = f"https://www.indeed.com/jobs?q={q}&l={l}&sort=date"

    print(f"\n[SEARCH] {keyword} | {country_name} ({country_code}) → {base_url}")

    driver.get(base_url)
    time.sleep(4)

    try:
        wait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'mosaic-provider-jobcards')]"))
        )
        print("Job list topildi.")
    except TimeoutException:
        print("[WARN] Job list topilmadi (CAPTCHA yoki blok).")
        return

    page = 0
    total_saved = 0

    while page < max_pages:
        page += 1
        print(f"  [PAGE] {page} | {country_name}")

        try:
            container = driver.find_element(By.XPATH, "//div[contains(@class,'mosaic-provider-jobcards')]")
            job_cards = container.find_elements(By.XPATH, ".//li[.//a[contains(@class,'jcs-JobTitle')]]")
        except:
            print("  [STOP] Kartalar topilmadi.")
            break

        if not job_cards:
            break

        for card in job_cards:
            try:
                title_link = card.find_element(By.XPATH, ".//a[contains(@class,'jcs-JobTitle')]")
                title = get_text_safe(title_link)
                if not title:
                    continue

                href = normalize_job_url(title_link.get_attribute("href") or "")
                job_id = get_job_id_from_url(href)
                if not job_id:
                    continue

                safe_click(driver, title_link)
                time.sleep(1.2)

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
                    country=country_name,
                    country_code=country_code,
                    source="indeed.com",
                )

                if saved:
                    total_saved += 1

            except Exception as e:
                print(f"  [CARD ERROR] {e}")
                continue

        if not click_next_or_stop(driver):
            print("  [STOP] Keyingi sahifa yo'q.")
            break

        time.sleep(2)

    print(f"[DONE] {keyword} | {country_name} → saved: {total_saved}")

# ----------------------------
# Main — Xatolarni yaxshiroq boshqarish
# ----------------------------
def main():
    driver = None
    conn = None
    try:
        driver = create_driver(headless=False)
        print("Brauzer ochildi.")

        driver.get(INDEED_HOME)
        time.sleep(3)

        if not login_google(driver):
            print("Login muvaffaqiyatsiz. Dastur to'xtatilmoqda.")
            return

        conn = open_db()
        ensure_indeed_table(conn)

        with open("jobs-list.json", "r", encoding="utf-8") as f:
            keywords = json.load(f)

        with open("countries.json", "r", encoding="utf-8") as f:
            countries = json.load(f)

        for keyword in keywords:
            keyword = str(keyword).strip()
            if not keyword:
                continue

            for country_name in countries:
                country_name = country_name.strip()
                country_code = COUNTRY_CODE_MAP.get(country_name, "")

                if not country_code:
                    print(f"[WARN] {country_name} uchun code topilmadi")

                scrape_keyword_country(driver, conn, keyword, country_name, country_code)
                time.sleep(12)

    except Exception as e:
        print(f"[MAIN ERROR] {e}")
        traceback.print_exc()
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
                print("Brauzer yopilishida xato (lekin zararsiz)")
        print("Dastur yakunlandi.")

if __name__ == "__main__":
    main()