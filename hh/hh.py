import datetime
import json
import os

import psycopg2
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ======================
# LOAD ENV
# ======================
load_dotenv()

# ======================
# DB CONNECTION
# ======================
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
conn.autocommit = True
cursor = conn.cursor()


# ======================
# CREATE TABLE
# ======================
def create_table_if_not_exists():
    cursor.execute("""
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
    """)


# ======================
# VALIDATORS
# ======================
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


# ======================
# SAVE DATA
# ======================
def save_to_database(data: dict):
    cursor.execute("""
        INSERT INTO hh (
            job_id, job_title, location, skills, salary,
            education, job_type, company_name, job_url,
            source, posted_date, job_subtitle
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (job_id) DO NOTHING;
    """, (
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
        data["job_subtitle"]
    ))


# ======================
# CHROME DRIVER
# ======================
def create_driver():
    options = uc.ChromeOptions()

    if os.getenv("HEADLESS", "false").lower() == "true":
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    return uc.Chrome(options=options)


# ======================
# SAFE TEXT
# ======================
def safe_text(driver, xpath):
    try:
        return driver.find_element(By.XPATH, xpath).text.strip()
    except NoSuchElementException:
        return ""


# ======================
# MAIN SCRAPER
# ======================
def get_hh_vacancies(jobs_list):
    driver = create_driver()
    wait = WebDriverWait(driver, 20)

    try:
        for job in jobs_list:
            page = 0

            while True:
                search_url = (
                    f"https://tashkent.hh.uz/search/vacancy"
                    f"?text={job}&page={page}"
                )
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
                        "posted_date": datetime.date.today(),
                        "job_subtitle": job
                    }

                    save_to_database(data)
                    print(f"SAVED: {job_id}")

                page += 1

    finally:
        driver.quit()
        cursor.close()
        conn.close()


# ======================
# ENTRY POINT
# ======================
def main():
    create_table_if_not_exists()

    with open("job_list.json", "r", encoding="utf-8") as f:
        jobs = json.load(f)

    get_hh_vacancies(jobs)
    print("DONE")


if __name__ == "__main__":
    main()
