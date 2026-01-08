# Itic_Jobs — Global Job Vacancies Scraper

**Itic_Jobs** is a scalable Python project for scraping job vacancies from the
**most popular job websites worldwide** and storing the data in a centralized
database for analytics, reporting, and further processing.

This project is designed to support **multiple job sources**, where each website
is implemented as a **separate module** (for example: HH, Indeed, LinkedIn,
Glassdoor, etc.).

---

## Project Goals

- Collect job vacancies from **global job platforms**
- Normalize job data into a single database schema
- Enable job market analysis across countries and platforms
- Provide a modular architecture for easy expansion
- Prevent duplicate job records

---

## Supported Sources (Modules)

Each job website is implemented as a **separate scraper module**.

- HH (HeadHunter Uzbekistan)

---

## Installation

Clone the repository:

```bash
git clone https://github.com/krv006/Itic_Jobs.git
cd Itic_Jobs
```

Install dependencies:

```bash
pip install -r requirements.txt
```

#### .env settings
```text
DB_DRIVER={ODBC Driver 17 for SQL Server}
DB_SERVER=
DB_NAME=

# Windows Authentication
DB_TRUSTED_CONNECTION=yes
```

---

## HH (HeadHunter Uzbekistan)

The **HH module** is responsible for scraping job vacancies from  
**HH.uz (HeadHunter Uzbekistan)**.

It is implemented as an independent scraper and follows the common database
schema used across the project.

---

### Module Structure

```text
Itic_Jobs/
│
├── hh/
│   └── hh.py        # HH.uz scraping logic
```

---

### Source Identifier

```text
source = "hh"
```

---

### Database Table (HH)

```sql
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
```

---

### Run HH Scraper

```bash
python hh/hh.py
```

---

## Future Plans

- Add more global job platforms (Indeed, LinkedIn, Glassdoor, etc.)
- Introduce a shared BaseScraper architecture
- Centralize database logic
- Export data to CSV and BI tools (Power BI, Azure Fabric)
- Add scheduling, logging, and retry mechanisms

---

## Legal Notice

This project is intended for **educational and research purposes**.
Always ensure compliance with the terms of service of the scraped websites.

---

## Author

**krv006**  
GitHub: https://github.com/krv006
