import datetime
import json
import os
import re

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)
conn.autocommit = True
cursor = conn.cursor()


def create_table_if_not_exists():
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS hh (
            job_id TEXT PRIMARY KEY,
            job_title TEXT,
            location TEXT,
            skills TEXT,
            salary TEXT,
            education TEXT,
            job_type TEXT,
            company_name TEXT,
            job_url TEXT,
            source TEXT,
            posted_date DATE,
            job_subtitle TEXT
        );
        """
    )


def is_valid_job_id(job_id: str) -> bool:
    return job_id.isdigit() and len(job_id) >= 6


def is_valid_job_title(title: str) -> bool:
    if not title or len(title) < 5:
        return False

    bad_words = (
        "найдено",
        "vacancy",
        "employers",
        "работодател",
        "ооо ",
        "тоо ",
        "ип ",
        "ao ",
        "ltd",
    )

    title_lower = title.lower()
    return not any(bad in title_lower for bad in bad_words)


def save_to_database(data: dict):
    cursor.execute(
        """
        INSERT INTO hh (
            job_id, job_title, location, skills, salary,
            education, job_type, company_name, job_url,
            source, posted_date, job_subtitle
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (job_id) DO NOTHING;
        """,
        (
            data["job_id"],
            data["job_title"],
            data["location"],
            data["skills"],
            data["salary"],
            data["education"],
            data["job_type"],
            data["company_name"],
            data["job_url"],
            data["source"],
            data["posted_date"],
            data["job_subtitle"],
        ),
    )


def create_driver():
    options = uc.ChromeOptions()

    if os.getenv("HEADLESS", "false").lower() == "true":
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return uc.Chrome(options=options)


def safe_text(driver, xpath: str) -> str:
    try:
        return driver.find_element(By.XPATH, xpath).text.strip()
    except NoSuchElementException:
        return ""


# ----------------------------
# ✅ HH Posted date parser
# ----------------------------
_RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def parse_posted_date_from_text(text: str) -> datetime.date | None:
    """
    Supports:
    - 'Вакансия опубликована 23 января 2026 ...'
    - 'Опубликовано: 23 января 2026'
    - 'сегодня', 'вчера'
    - '2 дня назад', '5 дней назад', '1 день назад'
    """
    if not text:
        return None

    t = text.strip().lower()

    today = datetime.date.today()

    if "сегодня" in t:
        return today
    if "вчера" in t:
        return today - datetime.timedelta(days=1)

    m = re.search(r"(\d+)\s*(дн(?:я|ей)|день)\s*назад", t)
    if m:
        days = int(m.group(1))
        return today - datetime.timedelta(days=days)

    m = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", t)
    if m:
        day = int(m.group(1))
        mon_name = m.group(2)
        year = int(m.group(3))
        mon = _RU_MONTHS.get(mon_name)
        if mon:
            return datetime.date(year, mon, day)

    return None


def get_hh_posted_date(driver) -> datetime.date:
    """
    Tries multiple places: visible text + page_source regex.
    Falls back to today only if nothing found.
    """
    candidates = []

    # HH ko‘pincha shunaqa data-qa lar ishlatadi (turli layoutlarda farq qiladi)
    xpaths = [
        "//*[@data-qa='vacancy-view-creation-time']",
        "//*[contains(text(),'Вакансия опубликована')]",
        "//*[contains(text(),'Опубликовано')]",
    ]

    for xp in xpaths:
        txt = safe_text(driver, xp)
        if txt:
            candidates.append(txt)

    # page_source ichidan ham qidiramiz (eng “robust” yo‘l)
    html = driver.page_source or ""
    m = re.search(r"(Вакансия опубликована[^<]{0,120})", html, flags=re.IGNORECASE)
    if m:
        candidates.append(m.group(1))

    m2 = re.search(r"(Опубликовано[^<]{0,120})", html, flags=re.IGNORECASE)
    if m2:
        candidates.append(m2.group(1))

    for c in candidates:
        dt = parse_posted_date_from_text(c)
        if dt:
            return dt

    # fallback (faqat topilmasa)
    return datetime.date.today()


def get_hh_vacancies(jobs_list):
    driver = create_driver()
    wait = WebDriverWait(driver, 20)

    try:
        for job in jobs_list:
            page = 0

            while True:
                search_url = f"https://tashkent.hh.uz/search/vacancy?text={job}&page={page}"
                driver.get(search_url)

                try:
                    job_links = wait.until(
                        EC.presence_of_all_elements_located(
                            (By.XPATH, "//a[contains(@class,'magritte-link')]")
                        )
                    )
                except TimeoutException:
                    break

                urls = [
                    a.get_attribute("href")
                    for a in job_links
                    if a.get_attribute("href")
                ]

                if not urls:
                    break

                for url in urls:
                    if not isinstance(url, str):
                        continue

                    driver.get(url)

                    job_id = url.split("?")[0].split("/")[-1]
                    if not is_valid_job_id(job_id):
                        continue

                    job_title = safe_text(driver, "//h1")
                    if not is_valid_job_title(job_title):
                        continue

                    posted_date = get_hh_posted_date(driver)

                    data = {
                        "job_id": job_id,
                        "job_title": job_title,
                        "location": safe_text(driver, "//span[@data-qa='vacancy-view-raw-address']"),
                        "skills": safe_text(driver, "//ul[contains(@class,'vacancy-skill-list')]").replace("\n", ","),
                        "salary": safe_text(driver, "//span[contains(@data-qa,'vacancy-salary')]"),
                        "education": "",
                        "job_type": safe_text(driver, "//div[@data-qa='vacancy-working-hours']"),
                        "company_name": safe_text(driver, "//div[@data-qa='vacancy-company__details']"),
                        "job_url": url,
                        "source": "hh.uz",
                        "posted_date": posted_date,   # ✅ endi HH dan oladi
                        "job_subtitle": job,
                    }

                    save_to_database(data)
                    print(f"SAVED: {job_id} | posted_date={posted_date}")

                page += 1

    finally:
        driver.quit()
        cursor.close()
        conn.close()


def main():
    create_table_if_not_exists()

    with open("job_list.json", "r", encoding="utf-8") as f:
        jobs = json.load(f)

    get_hh_vacancies(jobs)
    print("DONE")


if __name__ == "__main__":
    main()
