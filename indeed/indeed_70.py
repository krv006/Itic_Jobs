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
from selenium.common import NoSuchWindowException
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

INDEED_HOME = "https://www.indeed.com/"
DEFAULT_WAIT = 15

# ✅ ISO3 codes (3 harf)
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
    "Dubai": "ARE",  # UAE
    "Abu Dhabi": "ARE",
    "Uzbekistan": "UZB",
    "Kazakhstan": "KAZ",
}


# =========================
# TEXT HELPERS
# =========================
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def get_text_safe(el) -> str:
    try:
        return clean_text(el.text or "")
    except:
        return ""


def safe_click(driver, element) -> bool:
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


def first_existing(driver_or_el, selectors, timeout=4):
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


def wait(driver, t=DEFAULT_WAIT):
    return WebDriverWait(driver, t)


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


# =========================
# SALARY EXTRACT
# =========================
SALARY_RE = re.compile(
    r"(?P<cur>[$£€])\s?(?P<a>\d{1,3}(?:,\d{3})*(?:\.\d+)?)"
    r"(?:\s*(?:-|—|to)\s*(?P<cur2>[$£€])?\s?(?P<b>\d{1,3}(?:,\d{3})*(?:\.\d+)?))?"
    r"(?:\s*(?:an?\s*)?(?P<period>hour|hr|day|week|month|year))?",
    re.IGNORECASE,
)


def is_probably_big_description(txt: str) -> bool:
    if not txt:
        return False
    t = txt.lower()
    if len(txt) > 160:
        return True
    bad = ["full job description", "essential duties", "responsibilities", "education/experience"]
    return any(x in t for x in bad)


def extract_salary_from_text(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""
    m = SALARY_RE.search(text)
    if not m:
        return ""

    cur = m.group("cur")
    a = m.group("a")
    b = m.group("b")
    period = (m.group("period") or "").lower().strip()

    if b:
        out = f"{cur}{a} - {cur}{b}"
    else:
        out = f"{cur}{a}"

    if period:
        if period == "hr":
            period = "hour"
        if period == "hour":
            out += " an hour"
        else:
            out += f" a {period}"

    tail = text[m.end(): m.end() + 25].upper()
    if "USD" in tail:
        out += " USD"
    elif "GBP" in tail:
        out += " GBP"
    elif "EUR" in tail:
        out += " EUR"

    return out.strip()


# =========================
# POSTED DATE
# =========================
def parse_iso_date(s: str) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def parse_posted_date(raw_text: str) -> str | None:
    raw = clean_text((raw_text or "").lower())
    if not raw:
        return None

    raw = raw.replace("posted", "").strip()
    raw = raw.replace("employeractive", "active")
    raw = raw.replace("employer active", "active")

    if "just posted" in raw:
        return datetime.now().strftime("%Y-%m-%d")
    if "today" in raw:
        return datetime.now().strftime("%Y-%m-%d")

    m_plus = re.search(r"(\d+)\+\s*days\s*ago", raw)
    if m_plus:
        num = int(m_plus.group(1))
        dt = datetime.now() - timedelta(days=num)
        return dt.strftime("%Y-%m-%d")

    m = re.search(r"(\d+)\s*(day|days|hour|hours)\s*ago", raw)
    if m:
        num = int(m.group(1))
        unit = m.group(2)
        if "day" in unit:
            dt = datetime.now() - timedelta(days=num)
        else:
            dt = datetime.now() - timedelta(hours=num)
        return dt.strftime("%Y-%m-%d")

    formats = ["%b %d", "%b %d, %Y", "%B %d, %Y"]
    current_year = datetime.now().year
    for fmt in formats:
        try:
            dt = datetime.strptime(raw.title(), fmt)
            if dt.year == 1900:
                dt = dt.replace(year=current_year)
            return dt.strftime("%Y-%m-%d")
        except:
            pass

    return None


def extract_posted_date_from_jsonld(driver) -> str | None:
    try:
        scripts = driver.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")
    except:
        scripts = []

    for sc in scripts:
        try:
            txt = (sc.get_attribute("innerText") or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            items = data if isinstance(data, list) else [data]

            for item in items:
                if not isinstance(item, dict):
                    continue

                dp = item.get("datePosted") or item.get("datePublished")
                iso = parse_iso_date(dp) if dp else None
                if iso:
                    return iso

                graph = item.get("@graph")
                if isinstance(graph, list):
                    for g in graph:
                        if isinstance(g, dict):
                            dp2 = g.get("datePosted") or g.get("datePublished")
                            iso2 = parse_iso_date(dp2) if dp2 else None
                            if iso2:
                                return iso2
        except:
            continue

    return None


def create_driver(headless: bool = False):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1400,900")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    # ✅ Chrome 144 uchun majburan
    driver = uc.Chrome(
        options=options,
        use_subprocess=True,
        version_main=144
    )
    driver.set_page_load_timeout(60)
    return driver

def wait_for_human_verification(driver, timeout=180):
    """
    Agar Cloudflare/verification chiqsa, user qo'lda checkbox bosib o'tadi.
    Kod esa o'tguncha kutadi.
    """
    end = time.time() + timeout
    while time.time() < end:
        try:
            url = (driver.current_url or "").lower()
            title = (driver.title or "").lower()
            page = (driver.page_source or "").lower()

            # Cloudflare / verify detect
            if ("cloudflare" in page) or ("additional verification required" in page) or ("verify you are human" in page) or ("just a moment" in title):
                print("🧩 Cloudflare verification ko‘rindi. Iltimos brauzerda checkboxni bosib o‘ting...")
                time.sleep(3)
                continue

            # Agar sahifa normal bo'lsa chiqamiz
            return True
        except Exception:
            time.sleep(1)

    print("⏰ Verification timeout. Qo‘lda o‘tishga ulgurmadiz.")
    return False

def maybe_wait_for_cloudflare(driver):
    """
    Cloudflare / "Additional Verification Required" chiqsa:
    - kodni to‘xtatadi
    - siz qo‘lda checkboxni bosasiz
    - Enter bosib davom etasiz
    """
    try:
        time.sleep(2)
        title = (driver.title or "").lower()
        body = ""
        try:
            body = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        except:
            pass

        if "additional verification required" in body or "verify you are human" in body:
            print("\n⚠️ Cloudflare tekshiruv chiqdi.")
            print("👉 Brauzerda checkboxni bosib o‘ting, keyin shu konsolda Enter bosing...")
            input()
    except:
        pass


# =========================
# DB
# =========================
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
        conn = psycopg2.connect(host=host, port=int(port), dbname=dbname, user=user, password=password)
    conn.autocommit = False
    return conn


def ensure_indeed_table(conn):
    """
    ✅ Table create
    ✅ posted_date add if missing
    ✅ country_code always VARCHAR(3)
    ✅ search_query TEXT add if missing  <<< SIZ SO‘RAGAN
    """
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
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ux_indeed_jobid_source UNIQUE (job_id, source)
            );
            """
            cur.execute(create_sql)
            conn.commit()
            print("✅ DB ready (table created).")
            return

        # posted_date missing?
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='indeed' AND column_name='posted_date';
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE indeed ADD COLUMN posted_date DATE;")
            conn.commit()
            print("✅ Migration: posted_date added")

        # country_code length check
        cur.execute("""
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='indeed' AND column_name='country_code';
        """)
        row = cur.fetchone()
        if not row:
            cur.execute("ALTER TABLE indeed ADD COLUMN country_code VARCHAR(3);")
            conn.commit()
            print("✅ Migration: country_code added")
        else:
            max_len = row[0]
            if max_len is not None and int(max_len) < 3:
                cur.execute("ALTER TABLE indeed ALTER COLUMN country_code TYPE VARCHAR(3);")
                conn.commit()
                print("✅ Migration: country_code -> VARCHAR(3)")

        # ✅ search_query missing?
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='indeed' AND column_name='search_query';
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE indeed ADD COLUMN search_query TEXT;")
            conn.commit()
            print("✅ Migration: search_query added")

        print("✅ DB ready")

    except Error as e:
        conn.rollback()
        print(f"❌ ensure_indeed_table xato: {e}")
        traceback.print_exc()
    finally:
        cur.close()


def save_to_database(
        conn,
        job_id,
        search_query,
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
        posted_date=None,
        source="indeed.com",
):
    job_id = (job_id or "").strip()
    if not job_id:
        return False

    country_code = (country_code or "").strip()
    if country_code and len(country_code) > 3:
        country_code = country_code[:3]

    search_query = (search_query or "").strip()

    sql = """
    INSERT INTO indeed (
        job_id, source, search_query,
        job_title, company_name, location,
        salary, job_type, skills, education,
        job_url, country, country_code, posted_date
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_id, source) DO NOTHING;
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, (
            job_id, source, search_query,
            job_title, company_name, location,
            salary, job_type, skills, education,
            job_url, country, country_code, posted_date
        ))
        inserted = cur.rowcount == 1
        conn.commit()
        if inserted:
            print(f"  ✅ Saqlandi: {job_title[:55]} | {country_code} | {posted_date} | search={search_query}")
        return inserted
    except Exception as e:
        conn.rollback()
        print(f"DB ERROR: {e}")
        return False


# =========================
# LOGIN GOOGLE
# =========================
def login_google(driver) -> bool:
    print("🔐 Google login...")

    driver.get(INDEED_HOME)
    time.sleep(2)
    maybe_wait_for_cloudflare(driver)

    try:
        wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        print("❌ Sahifa yuklanmadi (timeout).")
        return False

    # Sign in
    try:
        sign_in = wait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(., 'Sign in') or contains(., 'Sign In') or contains(., 'Log in')]"))
        )
        safe_click(driver, sign_in)
        time.sleep(2)
        maybe_wait_for_cloudflare(driver)
    except TimeoutException:
        print("❌ Sign in tugmasi topilmadi.")
        return False

    # Google button
    try:
        google_btn = wait(driver, 20).until(EC.element_to_be_clickable((By.ID, "login-google-button")))
        safe_click(driver, google_btn)
        time.sleep(2)
    except TimeoutException:
        print("❌ Google tugmasi topilmadi.")
        return False

    # popup
    opened = False
    for _ in range(3):
        try:
            wait(driver, 15).until(lambda d: len(d.window_handles) > 1)
            driver.switch_to.window(driver.window_handles[-1])
            opened = True
            break
        except TimeoutException:
            time.sleep(2)

    if not opened:
        print("❌ Google oynasi ochilmadi.")
        return False

    email = _env_required("EMAIL")
    password = _env_required("EMAIL_PASSWORD")

    try:
        email_inp = wait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='email' or @name='identifier']"))
        )
        email_inp.clear()
        email_inp.send_keys(email)
        email_inp.send_keys(Keys.ENTER)
        time.sleep(3)

        pwd_inp = wait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@type='password']"))
        )
        pwd_inp.clear()
        pwd_inp.send_keys(password)
        pwd_inp.send_keys(Keys.ENTER)
        time.sleep(6)

        driver.switch_to.window(driver.window_handles[0])
        time.sleep(2)
        print("✅ Login OK")
        return True

    except Exception as e:
        print(f"❌ Login xato: {e}")
        traceback.print_exc()
        return False


# =========================
# READ DETAILS
# =========================
def read_job_details_from_right_panel(driver):
    panel = driver
    for sel in ["#jobsearch-ViewjobPaneWrapper", "div.jobsearch-RightPane", "div.jobsearch-JobComponent"]:
        try:
            panel = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except:
            pass

    company = ""
    try:
        company_el = first_existing(panel, [(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyName']")], timeout=2)
        if company_el:
            company = get_text_safe(company_el)
    except:
        pass

    location = ""
    try:
        loc_el = first_existing(panel, [(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']")], timeout=2)
        if loc_el:
            location = get_text_safe(loc_el)
    except:
        pass

    job_type = ""
    try:
        jt_el = first_existing(panel, [(By.XPATH, ".//*[contains(@aria-label, 'Job type')]")], timeout=2)
        if jt_el:
            job_type = get_text_safe(jt_el).replace("Job type", "").strip()
    except:
        pass

    skills = ""
    try:
        sk_el = first_existing(panel, [(By.CSS_SELECTOR, "[aria-label*='Skills'] ul, ul.js-match-insights-provider")],
                               timeout=2)
        if sk_el:
            raw = get_text_safe(sk_el)
            raw = raw.replace("Skills", "").replace("(Required)", "")
            parts = [p.strip() for p in raw.split("\n") if p.strip() and "Do you have" not in p]
            skills = ", ".join(parts)
    except:
        pass

    education = "No Degree Required"
    try:
        ed_el = first_existing(panel, [(By.CSS_SELECTOR, "[aria-label*='Education']")], timeout=2)
        if ed_el:
            raw = get_text_safe(ed_el).replace("Education", "").replace("(Required)", "")
            parts = [p.strip() for p in raw.split("\n") if p.strip() and "Do you have" not in p]
            if parts:
                education = ", ".join(parts)
    except:
        pass

    posted_date = None
    try:
        posted_date = extract_posted_date_from_jsonld(driver)
    except:
        posted_date = None

    if not posted_date:
        try:
            posted_date = parse_posted_date(get_text_safe(panel))
        except:
            pass

    salary = ""
    try:
        candidates = []
        cur_els = panel.find_elements(By.XPATH, ".//*[contains(., '$') or contains(., '£') or contains(., '€')]")
        for el in cur_els[:50]:
            txt = get_text_safe(el)
            if not txt:
                continue
            if is_probably_big_description(txt):
                continue
            if len(txt) > 140:
                continue
            candidates.append(txt)

        for c in candidates:
            s = extract_salary_from_text(c)
            if s:
                salary = s
                break

        if not salary:
            salary = extract_salary_from_text(get_text_safe(panel))
        salary = clean_text(salary)
    except:
        salary = ""

    return company, location, salary, job_type, skills, education, posted_date


# =========================
# PAGINATION
# =========================
def click_next_or_stop(driver) -> bool:
    selectors = [
        (By.CSS_SELECTOR, "[data-testid='pagination-page-next']"),
        (By.CSS_SELECTOR, "a[aria-label*='Next']"),
        (By.XPATH, "//a[contains(@aria-label,'Next')]"),
    ]
    for by, sel in selectors:
        try:
            el = wait(driver, 8).until(EC.element_to_be_clickable((by, sel)))
            return safe_click(driver, el)
        except:
            pass
    return False


# =========================
# SCRAPER
# =========================
def scrape_keyword_country(driver, conn, keyword: str, country_name: str, country_code: str = "", max_pages: int = 5):
    q = urllib.parse.quote_plus(keyword)
    l = urllib.parse.quote_plus(country_name)
    base_url = f"https://www.indeed.com/jobs?q={q}&l={l}&sort=date"

    # ✅ SIZ SO‘RAGAN "search_query" (ENG ODDI)
    search_query = f"{keyword} | {country_name}"

    print(f"\n🔎 {search_query}")
    driver.get(base_url)
    time.sleep(2)
    maybe_wait_for_cloudflare(driver)

    try:
        wait(driver, 25).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'mosaic-provider-jobcards')]"))
        )
    except TimeoutException:
        print("[WARN] Job list topilmadi (blok/captcha bo‘lishi mumkin).")
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

        for idx in range(len(job_cards)):
            try:
                container = driver.find_element(By.XPATH, "//div[contains(@class,'mosaic-provider-jobcards')]")
                job_cards = container.find_elements(By.XPATH, ".//li[.//a[contains(@class,'jcs-JobTitle')]]")
                if idx >= len(job_cards):
                    break

                card = job_cards[idx]
                title_link = card.find_element(By.XPATH, ".//a[contains(@class,'jcs-JobTitle')]")
                title = get_text_safe(title_link)
                if not title:
                    continue

                posted_date_raw = ""
                try:
                    posted_el = card.find_element(
                        By.XPATH,
                        ".//*[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'posted') "
                        "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'just posted') "
                        "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'active') "
                        "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'days ago') "
                        "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'today') "
                        "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'30+')]"
                    )
                    posted_date_raw = get_text_safe(posted_el)
                except:
                    pass

                card_posted = parse_posted_date(posted_date_raw) if posted_date_raw else None

                href = normalize_job_url(title_link.get_attribute("href") or "")
                job_id = get_job_id_from_url(href)
                if not job_id:
                    continue

                safe_click(driver, title_link)

                # panel wait (ba'zan yo‘q bo‘ladi, shuning uchun fail qilmaymiz)
                time.sleep(1.0)
                maybe_wait_for_cloudflare(driver)

                company, location, salary, job_type, skills, education, panel_posted = read_job_details_from_right_panel(
                    driver)
                posted_date = panel_posted or card_posted

                saved = save_to_database(
                    conn,
                    job_id=job_id,
                    search_query=search_query,  # ✅ mana shu DBga ketadi
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
                    posted_date=posted_date,
                    source="indeed.com",
                )

                if saved:
                    total_saved += 1

            except (StaleElementReferenceException,):
                continue
            except Exception as e:
                print(f"CARD ERROR: {e}")
                continue

        if not click_next_or_stop(driver):
            print("  [STOP] Keyingi sahifa yo'q.")
            break

        time.sleep(2)

    print(f"[DONE] {search_query} → saved: {total_saved}")


def main():
    conn = None
    driver = None
    try:
        conn = open_db()
        ensure_indeed_table(conn)
        print("✅ DB ready")

        # ✅ driver open + retry if window closed
        for attempt in range(2):
            try:
                driver = create_driver(headless=False)
                driver.get(INDEED_HOME)

                # Cloudflare bo'lsa qo'lda o'tasiz
                if not wait_for_human_verification(driver, timeout=180):
                    return

                if not login_google(driver):
                    print("❌ Login bo‘lmadi.")
                    return

                break  # login ok
            except NoSuchWindowException:
                print("⚠️ Window yopilib qoldi. Driver qayta ochilyapti...")
                try:
                    if driver:
                        driver.quit()
                except:
                    pass
                driver = None
                continue

        if not driver:
            print("❌ Driver ochilmadi.")
            return

        # ... qolgan scraping logic shu yerda davom etadi ...

    except Exception as e:
        print(f"[MAIN ERROR] {e}")
        traceback.print_exc()
    finally:
        if conn:
            try: conn.close()
            except: pass
        if driver:
            try: driver.quit()
            except: pass
        print("✅ Dastur yakunlandi.")


if __name__ == "__main__":
    main()
