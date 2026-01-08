import os
import datetime
import json
import time

import pyodbc
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.webdriver.common.by import By

load_dotenv()


def save_to_database(job_id, job_title, location, skills, salary, education, job_type,
                     company_name, job_url, source, posted_date, job_subtitle):
    try:
        conn = pyodbc.connect(
            f"Driver={os.getenv('DB_DRIVER')};"
            f"Server={os.getenv('DB_SERVER')};"
            f"Database={os.getenv('DB_NAME')};"
            f"Trusted_Connection={os.getenv('DB_TRUSTED_CONNECTION')};"
        )

        cursor = conn.cursor()

        insert_query = """
        INSERT INTO hh (
            job_id, job_title, location, skills, salary,
            education, job_type, company_name, job_url,
            source, posted_date, job_subtitle
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(insert_query, (
            job_id, job_title, location, skills, salary,
            education, job_type, company_name,
            job_url, source, posted_date, job_subtitle
        ))

        conn.commit()
        print(f"Saved job {job_id} to database.")

    except Exception as e:
        print(f"Error saving to database: {e}")

    finally:
        if 'conn' in locals():
            conn.close()


def create_driver():
    options = uc.ChromeOptions()
    # options.add_argument("--headless")  # Uncomment if you want headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")

    driver = uc.Chrome(options=options)
    return driver


def get_hh_vacancies(jobs_list):
    driver = create_driver()
    driver1 = create_driver()
    try:
        shart = 1
        page = 0
        job_ind = 0
        while shart:
            job = jobs_list[job_ind]
            driver.get(
                f"https://tashkent.hh.uz/search/vacancy?text={job}&page={page}&hhtmFrom=main&hhtmFromLabel=vacancy_search_line")
            job_elements = driver.find_elements(By.XPATH,
                                                "//div[@class='magritte-redesign']//h2[@data-qa='bloko-header-2']//a[contains(@class,'magritte-link')]")
            job_urls = [job_element.get_attribute("href") for job_element in job_elements]
            if job_urls == []:
                job_ind += 1
            else:
                for job_url in job_urls:
                    driver1.get(job_url)
                    time.sleep(2)
                    try:
                        title = driver1.find_element(By.XPATH, "//h1[@class='bloko-header-section-1']").text
                    except:
                        title = ""
                    try:
                        working_hours = driver1.find_element(By.XPATH, "//div[@data-qa='vacancy-working-hours']").text
                    except:
                        working_hours = ""
                    try:
                        company_name = driver1.find_element(By.XPATH, "//div[@data-qa='vacancy-company__details']").text
                    except:
                        company_name = ""
                    try:
                        skills = ",".join(
                            driver1.find_element(By.XPATH, "//ul[contains(@class,'vacancy-skill-list')]").text.split(
                                "\n"))
                    except:
                        skills = ""
                    try:
                        salary = driver1.find_element(By.XPATH, "//span[contains(@data-qa,'vacancy-salary')]").text
                    except:
                        salary = ""
                    try:
                        working_schedule = driver1.find_element(By.XPATH,
                                                                "//p[@data-qa='work-schedule-by-days-text']").text
                    except:
                        working_schedule = ""
                    try:
                        work_format = driver1.find_element(By.XPATH, "//span[@data-qa='work-formats-text']").text
                    except:
                        work_format = ""
                    try:
                        location = driver1.find_element(By.XPATH,
                                                        "//span[@data-qa='vacancy-view-raw-address']").text.replace(
                            "\n", "")
                    except:
                        location = ""
                    try:
                        posted_date = datetime.datetime.strptime(
                            driver1.find_element(By.XPATH, "//p[@data-qa='vacancy-creation-time']/span").text.replace(
                                "on", "").strip(), "%B %d, %Y")
                    except:
                        posted_date = datetime.datetime.now()
                    try:
                        job_id = driver1.current_url.split("?")[0].split("/")[-1]
                        save_to_database(
                            job_id=int(job_id),
                            job_title=title,
                            location=location,
                            skills=skills,
                            salary=salary,
                            education="",
                            job_type=working_hours,
                            company_name=company_name,
                            job_url=job_url,
                            source="hh.uz",
                            posted_date=posted_date.strftime("%Y-%m-%d"),
                            job_subtitle=job
                        )
                    except:
                        continue
                page += 1


    finally:
        driver.quit()
        driver1.quit()


def main():
    with open("job_list.json", "r") as file:
        jobs_list = json.load(file)
    get_hh_vacancies(jobs_list)
    print("Vacancies fetched successfully.")


if __name__ == "__main__":
    main()

# TODO: Create a table for HH data in SSMS (DBO)
"""
CREATE TABLE [dbo].[hh](
  [job_id] [nvarchar](100) NOT NULL,
  [job_title] [nvarchar](100) NULL,
  [location] [nvarchar](100) NULL,
  [skills] [nvarchar](max) NULL,
  [salary] [nvarchar](max) NULL,
  [education] [nvarchar](100) NULL,
  [job_type] [nvarchar](50) NULL,
  [company_name] [nvarchar](100) NULL,
  [job_url] [nvarchar](200) NULL,
  [source] [nvarchar](20) NULL,
  [description] [nvarchar](max) NULL,
  [job_subtitle] [nvarchar](250) NULL,
[posted_date] DATE NOT NULL, 
PRIMARY KEY CLUSTERED 
(
  [job_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

"""
