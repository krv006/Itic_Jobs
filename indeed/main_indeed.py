import json
import time

import pyodbc
import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchWindowException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


def create_driver(headless=False):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("start-maximized")

    driver = uc.Chrome(options=options, version_main=143)  # ✅ shu yer
    return driver


def login(driver):
    print("Logging into Indeed using Google...")

    driver.find_element(By.XPATH, "//a[contains(text(), 'Sign in')]").click()
    time.sleep(3)

    with open("credentials.json", "r") as file:
        creds = json.load(file)

    email = creds['email']
    password = creds['password']

    try:
        google_button = driver.find_element(By.ID, "login-google-button")
        google_button.click()
        time.sleep(5)
    except Exception as e:
        print("Google login button not found.")
        return False

    # Handle possible new window
    try:
        windows = driver.window_handles
        if len(windows) > 1:
            driver.switch_to.window(windows[-1])
        else:
            print("Google login window did not open.")
            return False

        # Login form
        email_input = driver.find_element(By.XPATH, "//input[@type='email']")
        email_input.send_keys(email)
        email_input.send_keys(Keys.ENTER)
        time.sleep(3)

        password_input = driver.find_element(By.XPATH, "//input[@type='password']")
        password_input.send_keys(password)
        password_input.send_keys(Keys.ENTER)
        time.sleep(5)

        driver.switch_to.window(driver.window_handles[0])  # Back to Indeed
        return True

    except NoSuchWindowException:
        print("Google login window was closed unexpectedly.")
        return False
    except Exception as e:
        print(f"Error during login: {e}")
        return False


def save_to_database(job_id, job_title, location, skills, salary, education, job_type, company_name, job_url, source):
    try:
        with open("conn.json") as file:
            conn_dt = json.load(file)
        driver = conn_dt['driver']
        server = conn_dt['server']
        db = conn_dt["db_name"]
        conn = pyodbc.connect(
            f"Driver={driver};"
            f"Server={server};"
            f"Database={db};"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO indeed (
            job_id, job_title, location, skills, salary, education, job_type, company_name, job_url, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            job_id, job_title, location, skills, salary, education, job_type, company_name, job_url, source,))
        conn.commit()
    except:
        pass


def scrape_jobs(driver, base_url):
    driver.get(base_url)
    data = {
        "job_title": [],
        "location": [],
        "skills": [],
        "salary": [],
        "education": [],
        "job_type": [],
        "company_name": [],
        "job_url": [],
        # "job_description": []
    }
    time.sleep(10)
    while True:
        time.sleep(2)
        jobs = driver.find_element(By.XPATH, "//div[contains(@class, 'mosaic-provider-jobcards')]").find_elements(
            By.XPATH, "//li")

        for job in jobs:
            # job.click()
            try:
                tt = job.find_element(By.XPATH, ".//a[contains(@class, 'jcs-JobTitle')]")
                tt.click()
                title = tt.text
            except:
                continue
            time.sleep(1)
            # location
            try:
                location = job.find_element(By.XPATH, "//div[@data-testid='inlineHeader-companyLocation']").text
            except:
                location = ""
            # salary
            try:
                salary = driver.find_element(By.XPATH, "//div[contains(@aria-label, 'Pay')]").text.replace("Pay",
                                                                                                           "").strip()
            except:
                salary = ""
            # job_type
            try:
                job_type = driver.find_element(By.XPATH, "//div[contains(@aria-label, 'Job type')]").text.replace(
                    "Job type", "").strip()
            except:
                job_type = ""

            # skills
            try:
                try:
                    driver.find_element(By.XPATH, "//button[contains(text(), '+ show more')]").click()
                    time.sleep(1)
                except:
                    pass
                skills = driver.find_element(By.XPATH, "//div[contains(@aria-label, 'Skills')]").find_element(By.XPATH,
                                                                                                              "//ul[contains(@class,'js-match-insights-provider')]").text
                sks = skills.replace("Skills", "").replace("+ show more", "").replace("- show less", "").replace(
                    "(Required)", "").replace("\n", ',').replace(",,", ',').split(",")
                skills = ','.join([sk for sk in sks if sk != '' and sk != " " and "Do you have" not in sk])
            except:
                skills = ''
            # education
            try:
                education = driver.find_element(By.XPATH, "//div[@aria-label='Education']").text.replace("Education",
                                                                                                         "").replace(
                    "(Required)", "").replace("\n", ",").replace(",,", ',')
                edcs = education.split(",")
                education = ','.join([ed for ed in edcs if ed != '' and ed != " " and "Do you have" not in ed])
            except:
                education = "No Degree Required"
            # company Name
            try:
                comp_name = driver.find_element(By.XPATH,
                                                "//div[contains(@data-testid, 'inlineHeader-companyName')]").text
            except:
                comp_name = ""
            # company Url
            try:
                comp_url = driver.find_element(By.XPATH,
                                               "//div[contains(@data-testid, 'inlineHeader-companyName')]").find_element(
                    By.XPATH, "//a[contains(@class,'serp-page')]").get_attribute("href")
            except:
                comp_url = ""
            job_id = driver.current_url.split("vjk=")[-1].split("&")[0]
            save_to_database(job_id, title, location, skills, salary, education, job_type, comp_name, comp_url,
                             "indeed.com")
        # df = pd.DataFrame(data)
        # df['job_id'] = df['job_url'].apply(lambda url: url.split("vjk=")[-1].split("&")[0])
        # df['source'] = ["indeed.com" for _ in range(len(df['company_name']))]
        # df.to_csv("ruslt.csv")
        driver.find_element(By.XPATH, "//a[@data-testid='pagination-page-next']").click()


def main():
    driver = create_driver(headless=False)
    driver.get("https://www.indeed.com/")
    time.sleep(5)

    if not login(driver):
        print("Login failed. Exiting.")
        driver.quit()
        return

    print("Login successful. Starting job scraping...")
    with open("jobs-list.json") as file:
        data = json.load(file)
    for job_n in data:
        base_url = f"https://www.indeed.com/jobs?q={job_n}&l=&sort=date&from=searchOnDesktopSerp"
        scrape_jobs(driver, base_url)

    driver.quit()


if __name__ == "__main__":
    main()
