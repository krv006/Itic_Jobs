# =========================
# INDEED SCRAPER (FULL FINAL)
# =========================

import json
import os
import re
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta
from html import unescape

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from psycopg2 import Error
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


# =========================
# INIT
# =========================

load_dotenv()

INDEED_HOME = "https://www.indeed.com/"
DEFAULT_WAIT = 15


COUNTRY_CODE_MAP = {
    "UK": "GBR",
    "London": "GBR",
    "Japan": "JPN",
    "Germany": "DEU",
    "Poland": "POL",
    "France": "FRA",
    "Switzerland": "CHE",
    "Philippines": "PHL",
    "United States": "USA",
    "China": "CHN",
    "Dubai": "ARE",
    "Abu Dhabi": "ARE",
    "Uzbekistan": "UZB",
    "Kazakhstan": "KAZ",
}


# =========================
# HELPERS
# =========================

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def get_text_safe(el):
    try:
        return clean_text(el.text or "")
    except:
        return ""


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


def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)


def normalize_job_url(href):
    if not href:
        return ""
    if href.startswith("/"):
        return "https://www.indeed.com" + href
    return href


def get_job_id_from_url(url):
    if "vjk=" in url:
        return url.split("vjk=")[-1].split("&")[0]
    if "jk=" in url:
        return url.split("jk=")[-1].split("&")[0]
    return ""


# =========================
# SALARY
# =========================

SALARY_RE = re.compile(
    r"([$£€])\s?(\d[\d,]*)(?:\s*[-–to]+\s*([$£€])?\s?(\d[\d,]*))?(?:.*?(hour|day|week|month|year|hr))?",
    re.I
)


def extract_salary(txt):
    txt = clean_text(txt)
    if len(txt) > 160:
        return ""

    m = SALARY_RE.search(txt)
    if not m:
        return ""

    cur = m.group(1)
    a = m.group(2)
    b = m.group(4)
    period = m.group(5)

    out = f"{cur}{a}"

    if b:
        out += f" - {cur}{b}"

    if period:
        if period == "hr":
            period = "hour"
        out += f" / {period}"

    return out


# =========================
# POSTED DATE
# =========================

def parse_posted_date(txt):
    txt = clean_text((txt or "").lower())

    if not txt:
        return None

    if "today" in txt or "just posted" in txt:
        return datetime.now().strftime("%Y-%m-%d")

    m = re.search(r"(\d+)\s*(day|hour)", txt)
    if m:
        num = int(m.group(1))
        unit = m.group(2)

        if unit == "day":
            dt = datetime.now() - timedelta(days=num)
        else:
            dt = datetime.now() - timedelta(hours=num)

        return dt.strftime("%Y-%m-%d")

    return None


def extract_posted_from_jsonld(driver):
    try:
        scripts = driver.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")
    except:
        return None

    for sc in scripts:
        try:
            data = json.loads(sc.get_attribute("innerText"))

            if isinstance(data, dict):
                dp = data.get("datePosted")
                if dp:
                    return dp[:10]

        except:
            continue

    return None


# =========================
# DRIVER
# =========================

def create_driver():
    options = uc.ChromeOptions()

    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)

    return driver


# =========================
# DB
# =========================

def env(key):
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f".env da {key} yo'q")
    return v


def open_db():
    return psycopg2.connect(
        host=env("DB_HOST"),
        port=env("DB_PORT"),
        dbname=env("DB_NAME"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
    )


def ensure_table(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS indeed (
        id BIGSERIAL PRIMARY KEY,
        job_id VARCHAR(100),
        source VARCHAR(50),
        search_query TEXT,
        job_title TEXT,
        company_name TEXT,
        location TEXT,
        salary TEXT,
        job_type TEXT,
        skills TEXT,
        education TEXT,
        job_url TEXT,
        country TEXT,
        country_code VARCHAR(3),
        posted_date DATE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(job_id, source)
    );
    """)

    conn.commit()
    cur.close()

    print("✅ DB ready")


def save_db(conn, data: dict):

    sql = """
    INSERT INTO indeed (
        job_id, source, search_query,
        job_title, company_name, location,
        salary, job_type, skills, education,
        job_url, country, country_code, posted_date
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (job_id, source) DO NOTHING;
    """

    cur = conn.cursor()

    try:
        cur.execute(sql, (
            data["job_id"],
            "indeed.com",
            data["query"],

            data["title"],
            data["company"],
            data["location"],

            data["salary"],
            data["job_type"],
            data["skills"],
            data["education"],

            data["url"],
            data["country"],
            data["code"],
            data["posted"],
        ))

        conn.commit()

        print(f"✅ Saved: {data['title'][:50]} | {data['query']}")

        return True

    except Exception as e:

        conn.rollback()
        print("DB ERROR:", e)

        return False

    finally:
        cur.close()


# =========================
# LOGIN
# =========================

def login_google(driver):

    print("🔐 Google login...")

    driver.get(INDEED_HOME)
    time.sleep(3)

    try:
        btn = wait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(.,'Sign in')]"))
        )
        btn.click()
        time.sleep(3)
    except:
        return False

    try:
        gbtn = wait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "login-google-button"))
        )
        gbtn.click()
        time.sleep(4)
    except:
        return False

    wait(driver, 10).until(lambda d: len(d.window_handles) > 1)
    driver.switch_to.window(driver.window_handles[-1])

    email = env("EMAIL")
    pwd = env("EMAIL_PASSWORD")

    wait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "identifier"))).send_keys(email + Keys.ENTER)
    time.sleep(3)

    wait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "Passwd"))).send_keys(pwd + Keys.ENTER)
    time.sleep(6)

    driver.switch_to.window(driver.window_handles[0])

    print("✅ Login OK")

    return True


# =========================
# PANEL PARSE
# =========================

def read_panel(driver):

    panel = driver.find_element(By.ID, "jobsearch-ViewjobPaneWrapper")

    company = get_text_safe(
        panel.find_element(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']")
    )

    location = get_text_safe(
        panel.find_element(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']")
    )

    salary = extract_salary(panel.text)

    posted = extract_posted_from_jsonld(driver) or parse_posted_date(panel.text)

    return company, location, salary, posted


# =========================
# SCRAPER
# =========================

def scrape(driver, conn, keyword, country, code):

    q = urllib.parse.quote_plus(keyword)
    l = urllib.parse.quote_plus(country)

    url = f"https://www.indeed.com/jobs?q={q}&l={l}&sort=date"

    print(f"\n🔎 {keyword} | {country}")

    driver.get(url)
    time.sleep(3)

    wait(driver, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, "mosaic-provider-jobcards"))
    )

    pages = 0

    while pages < 5:

        pages += 1

        cards = driver.find_elements(By.CSS_SELECTOR, "a.jcs-JobTitle")

        for i in range(len(cards)):

            try:

                cards = driver.find_elements(By.CSS_SELECTOR, "a.jcs-JobTitle")
                card = cards[i]

                title = get_text_safe(card)

                href = normalize_job_url(card.get_attribute("href"))

                job_id = get_job_id_from_url(href)

                if not job_id:
                    continue

                safe_click(driver, card)

                time.sleep(1)

                company, location, salary, posted = read_panel(driver)

                data = {
                    "job_id": job_id,
                    "query": keyword,
                    "title": title,
                    "company": company,
                    "location": location,
                    "salary": salary,
                    "job_type": "",
                    "skills": "",
                    "education": "",
                    "url": href,
                    "country": country,
                    "code": code,
                    "posted": posted,
                }

                save_db(conn, data)

            except StaleElementReferenceException:
                continue

            except Exception as e:
                print("CARD ERROR:", e)

        # next
        try:
            nxt = driver.find_element(By.CSS_SELECTOR, "[aria-label*='Next']")
            safe_click(driver, nxt)
            time.sleep(2)
        except:
            break


# =========================
# MAIN
# =========================

def main():

    driver = create_driver()
    conn = open_db()

    ensure_table(conn)

    if not login_google(driver):
        print("❌ Login failed")
        return

    with open("jobs-list.json") as f:
        keywords = json.load(f)

    with open("countries.json") as f:
        countries = json.load(f)

    for kw in keywords:

        kw = kw.strip()
        if not kw:
            continue

        for c in countries:

            c = c.strip()
            if not c:
                continue

            code = COUNTRY_CODE_MAP.get(c, "")

            scrape(driver, conn, kw, c, code)

            time.sleep(5)

    driver.quit()
    conn.close()

    print("✅ DONE")


if __name__ == "__main__":
    main()
