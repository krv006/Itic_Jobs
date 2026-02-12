import datetime
import hashlib
import json
import os
import re
import time
from pathlib import Path

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

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

# ================== COUNTRY -> ISO3 CODE ==================
COUNTRY_CODE_MAP = {
    "UK": "UK",
    "Japan": "JPN",
    "Germany": "DEU",
    "Poland": "POL",
    "France": "FRA",
    "Switzerland": "CHE",
    "London": "UK",
    "Philippines": "PHL",
    "United States": "USA",
    "China": "CHN",
    "Dubai": "AUE",
    "Abu Dhabi": "AUE",
    "Uzbekistan": "UZB",
    "Kazakhstan": "KAZ",
}


def get_country_code(country: str) -> str:
    return COUNTRY_CODE_MAP.get((country or "").strip(), "UNK")


# ================== SALARY (STRICT + DISPLAY) ==================
CURRENCY_SIGNS = ["R$", "A$", "C$", "HK$", "$", "€", "£", "¥", "₽", "₩", "₹", "₺", "₫", "฿", "₴", "₦"]
CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "SGD", "HKD", "RUB", "INR", "KRW", "BRL"]

CURRENCY_SYMBOL_MAP = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "RUB": "₽",
    "AUD": "A$",
    "CAD": "C$",
    "HKD": "HK$",
    "SGD": "S$",
    "KRW": "₩",
    "INR": "₹",
    "BRL": "R$",
    "UNK": "",
}


def detect_currency(raw: str) -> str:
    if not raw:
        return "UNK"
    t = raw.upper()
    if "A$" in raw or "AUD" in t: return "AUD"
    if "C$" in raw or "CAD" in t: return "CAD"
    if "HK$" in raw or "HKD" in t: return "HKD"
    if "S$" in raw or "SGD" in t: return "SGD"
    if "R$" in raw or "BRL" in t: return "BRL"
    if "$" in raw or "USD" in t: return "USD"
    if "€" in raw or "EUR" in t: return "EUR"
    if "£" in raw or "GBP" in t: return "GBP"
    if "¥" in raw or "JPY" in t: return "JPY"
    if "₽" in raw or "RUB" in t: return "RUB"
    if "₩" in raw or "KRW" in t: return "KRW"
    if "₹" in raw or "INR" in t: return "INR"
    return "UNK"


# 1) Valyuta belgisi bor salary (ishonchli)
SIGN_RANGE_RE = re.compile(
    r"""
    (?:R\$|A\$|C\$|HK\$|S\$|\$|€|£|¥|₽|₩|₹|₺|₫|฿|₴|₦)
    \s*\d+(?:[.,]\d+)?\s*[KM]?
    (?:\s*[-–—]\s*
        (?:R\$|A\$|C\$|HK\$|S\$|\$|€|£|¥|₽|₩|₹|₺|₫|฿|₴|₦)?
        \s*\d+(?:[.,]\d+)?\s*[KM]?
    )?
    """,
    re.VERBOSE
)

# 2) Currency code bilan
CODE_RANGE_RE = re.compile(
    r"""
    \b(?:USD|EUR|GBP|JPY|AUD|CAD|SGD|HKD|RUB|INR|KRW|BRL)\b
    \s*\d+(?:[.,]\d+)?\s*[KM]?
    (?:\s*[-–—]\s*\d+(?:[.,]\d+)?\s*[KM]?)?
    """,
    re.VERBOSE | re.IGNORECASE
)

# 3) Belgisiz range, faqat context bilan (3D/501 ni yo‘q qiladi)
CONTEXT_RANGE_RE = re.compile(
    r"""
    (?:pay|salary|estimated|estimate|compensation)
    [^\d]{0,50}
    (\d+(?:[.,]\d+)?\s*[KM]?)
    \s*[-–—]\s*
    (\d+(?:[.,]\d+)?\s*[KM]?)
    """,
    re.VERBOSE | re.IGNORECASE
)


def to_int(num_str: str, unit: str):
    v = float(num_str.replace(",", ""))
    unit = (unit or "").upper()
    if unit == "K":
        v *= 1000
    elif unit == "M":
        v *= 1000000
    return int(v)


def normalize_salary(raw_text: str):
    """
    OUTPUT: "GBP:25000-35000" / "USD:76000-101000" / "EUR:50000"
    """
    if not raw_text:
        return None

    txt = raw_text.replace("—", "-").replace("–", "-")

    # A) sign-based
    m = SIGN_RANGE_RE.search(txt)
    if m:
        snippet = m.group(0)
        cur = detect_currency(snippet)

        s = snippet
        for sym in ["R$", "A$", "C$", "HK$", "S$"]:
            s = s.replace(sym, "")
        for sym in ["$", "€", "£", "¥", "₽", "₩", "₹", "₺", "₫", "฿", "₴", "₦"]:
            s = s.replace(sym, "")
        s = s.replace(",", "").strip()

        nums = re.findall(r"(\d+(?:\.\d+)?)([KM]?)", s)
        if not nums:
            return None

        vals = [to_int(n, u) for (n, u) in nums[:2]]
        if len(vals) == 2:
            return f"{cur}:{vals[0]}-{vals[1]}"
        return f"{cur}:{vals[0]}"

    # B) code-based
    m = CODE_RANGE_RE.search(txt)
    if m:
        snippet = m.group(0)
        cur = detect_currency(snippet)

        s = re.sub(rf"\b({'|'.join(CURRENCY_CODES)})\b", "", snippet, flags=re.IGNORECASE)
        s = s.replace(",", "").strip()

        nums = re.findall(r"(\d+(?:\.\d+)?)([KM]?)", s)
        if not nums:
            return None

        vals = [to_int(n, u) for (n, u) in nums[:2]]
        if len(vals) == 2:
            return f"{cur}:{vals[0]}-{vals[1]}"
        return f"{cur}:{vals[0]}"

    # C) context-based
    m = CONTEXT_RANGE_RE.search(txt)
    if m:
        a, b = m.group(1), m.group(2)
        n1 = re.findall(r"(\d+(?:[.,]\d+)?)([KM]?)", a.replace(",", ""))[0]
        n2 = re.findall(r"(\d+(?:[.,]\d+)?)([KM]?)", b.replace(",", ""))[0]
        v1 = to_int(n1[0], n1[1])
        v2 = to_int(n2[0], n2[1])
        cur = detect_currency(txt)
        return f"{cur}:{v1}-{v2}"

    return None


def salary_norm_to_display(salary_norm: str):
    """
    "GBP:25000-35000" -> "£25000-£35000"
    "USD:76000-101000" -> "$76000-$101000"
    "EUR:50000" -> "€50000"
    """
    if not salary_norm:
        return None
    s = salary_norm.strip()
    if ":" not in s:
        return s

    code, rest = s.split(":", 1)
    code = code.strip().upper()
    sym = CURRENCY_SYMBOL_MAP.get(code, "")

    if "-" in rest:
        a, b = rest.split("-", 1)
        a, b = a.strip(), b.strip()
        return f"{sym}{a}-{sym}{b}" if sym else f"{a}-{b}"

    rest = rest.strip()
    return f"{sym}{rest}" if sym else rest


def get_salary_from_card_only_safe(driver, card_xpath: str):
    """
    Card text’dan:
    - valyuta belgisi bor bo‘lsa -> ok
    - yoki pay/salary keyword bo‘lsa -> ok
    aks holda -> None (3D/501 xatolar yo‘q bo‘ladi)
    """
    try:
        card_el = driver.find_element(By.XPATH, card_xpath)
        t = (card_el.text or "").strip()
    except:
        return None

    if not t:
        return None

    if any(sym in t for sym in CURRENCY_SIGNS):
        return normalize_salary(t)

    low = t.lower()
    if any(k in low for k in ["pay", "salary", "estimated", "estimate", "compensation"]):
        return normalize_salary(t)

    return None


def get_salary_from_details(driver, old_norm: str, timeout_sec: int = 4):
    """
    Details salary elementlaridan oladi.
    Old salary bilan bir xil bo‘lib qolmasligi uchun ozgina kutadi.
    """
    xps = [
        "//div[contains(@id,'job-salary')]",
        "//*[@data-test='detailSalary']",
        "//*[@data-test='detailSalaryInfo']",
        "//div[contains(@data-test,'detailSalary')]",
        "//span[contains(@data-test,'detailSalary')]",
    ]
    start = time.time()

    while time.time() - start < timeout_sec:
        for xp in xps:
            try:
                t = (driver.find_element(By.XPATH, xp).text or "").strip()
                if not t:
                    continue
                norm = normalize_salary(t)
                if norm and (not old_norm or norm != old_norm):
                    return norm
            except:
                pass
        time.sleep(0.25)

    # last try
    for xp in xps:
        try:
            t = (driver.find_element(By.XPATH, xp).text or "").strip()
            norm = normalize_salary(t)
            if norm:
                return norm
        except:
            pass

    return None


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
        country_code TEXT,
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


def save_to_database(title, company, location, location_sub, country_code, title_sub, skills, salary, date):
    job_hash = generate_job_hash(title, company, location)

    if salary is not None:
        salary = salary.strip()
        if salary == "":
            salary = None

    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO glassdoor
            (job_hash, title, company, location, location_sub, country_code, title_sub, skills, salary, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_hash) DO NOTHING;
            """,
            (job_hash, title, company, location, location_sub, country_code, title_sub, skills, salary, date)
        )
        conn.commit()

        if cur.rowcount == 0:
            print(f"⏭️ Duplicate skipped: {title} @ {company}")
            return False

        print(f"✅ Saved: {title} @ {company} | {country_code} | salary={salary}")
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
        self.country_code = get_country_code(country)

        self.driver = driver
        self.wait = WebDriverWait(driver, 12)
        self.wait1 = WebDriverWait(driver, 6)

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

        # stale panel fix: old title
        try:
            old_title = self.driver.find_element(By.XPATH, "//h1[contains(@id,'job-title')]").text.strip()
        except:
            old_title = ""

        # old salary norm (details)
        try:
            old_salary_raw = self.driver.find_element(By.XPATH, "//div[contains(@id,'job-salary')]").text.strip()
            old_salary_norm = normalize_salary(old_salary_raw)
        except:
            old_salary_norm = None

        # click card
        try:
            el = self.wait.until(EC.element_to_be_clickable((By.XPATH, base)))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
        except:
            return False

        # wait title changes
        try:
            self.wait.until(
                lambda d: (d.find_element(By.XPATH, "//h1[contains(@id,'job-title')]").text.strip() != old_title)
                          and (d.find_element(By.XPATH, "//h1[contains(@id,'job-title')]").text.strip() != "")
            )
        except:
            pass

        time.sleep(0.5)

        # card meta
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

        # details
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

        # ✅ salary: first from card (safe), else from details
        salary_norm = get_salary_from_card_only_safe(self.driver, base)
        if not salary_norm:
            salary_norm = get_salary_from_details(self.driver, old_salary_norm, timeout_sec=4)

        # ✅ add symbol display
        salary = salary_norm_to_display(salary_norm)

        # skills
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

        # date
        today = datetime.date.today()
        if "30" in posted_ago:
            date = today - datetime.timedelta(days=30)
        elif "d" in posted_ago.lower():
            digits = "".join(filter(str.isdigit, posted_ago))
            date = today - datetime.timedelta(days=int(digits)) if digits else today
        else:
            date = today

        save_to_database(
            title=title,
            company=company,
            location=location,
            location_sub=self.country,
            country_code=self.country_code,
            title_sub=self.job,
            skills=skills,
            salary=salary,
            date=date
        )
        return True


# ================== RUNNER ==================
if __name__ == "__main__":
    create_table_if_not_exists()

    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    driver = uc.Chrome(
        options=options,
        version_main=144
    )

    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    with open(COUNTRIES_PATH, "r", encoding="utf-8") as f:
        countries = json.load(f)

    for country in countries:
        for job in jobs:
            print(f"\n=== {job} | {country} ({get_country_code(country)}) ===")
            try:
                GlassdoorScraper(job, country, driver)
            except Exception as e:
                print("❌ Error:", e)

    driver.quit()
