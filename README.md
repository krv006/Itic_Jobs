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

### Module Structure HH

```text
Itic_Jobs/
│
├── hh/
│   └── hh.py        # HH.uz scraping logic
```

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

## IT-Market (Uzbekistan)

The **IT-Market module** is responsible for scraping job vacancies from  
**it-market.uz/job (IT-Market Uzbekistan)**.

It is implemented as an independent scraper and follows the common database
schema used across the project.
---

### Module Structure IT

```text
Itic_Jobs/
│
├── it_market/
│   └── it_park_job.py        # it-market.uz scraping logic
```

### Database Table IT Park

```sql
CREATE TABLE [dbo].[it_park ](
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

## Glassdoor (International)

The **Glassdoor module** is responsible for scraping job vacancies from  
**glassdoor.com (Global IT job market and company reviews)**.


### Module Structure Glassdoor

```text
Itic_Jobs/
│
├── glassdoor/
│   └── glassdoor_main.py        # glassdoor.com scraping logic
```

### Database Table Glassdoor

```sql
CREATE TABLE dbo.Glassdoor (
    id INT IDENTITY(1,1) PRIMARY KEY,
    job_hash CHAR(32) NOT NULL UNIQUE,
    title NVARCHAR(500),
    company NVARCHAR(255),
    location NVARCHAR(255),
    location_sub NVARCHAR(100),
    title_sub NVARCHAR(100),
    skills NVARCHAR(MAX),
    salary NVARCHAR(255),
    [date] DATE,
    created_at DATETIME DEFAULT GETDATE()
);
```


## Indeed 

The **Indeed module** is responsible for scraping job vacancies from  
**indeed.com (Global job market and employment opportunities platform)**.


### Module Structure Indeed

```text
Itic_Jobs/
│
├── indeed/
│   └── main_indeed.py        # indeed.com scraping logic
```

### Database Table Glassdoor

```sql
IF OBJECT_ID('dbo.indeed', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.indeed (
        id INT IDENTITY(1,1) PRIMARY KEY,
        job_id NVARCHAR(100) NOT NULL,
        source NVARCHAR(50) NOT NULL,
        job_title NVARCHAR(500) NULL,
        company_name NVARCHAR(500) NULL,
        location NVARCHAR(255) NULL,
        salary NVARCHAR(255) NULL,
        job_type NVARCHAR(255) NULL,
        skills NVARCHAR(MAX) NULL,
        education NVARCHAR(255) NULL,
        job_url NVARCHAR(1000) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );


    CREATE UNIQUE INDEX UX_indeed_jobid_source
        ON dbo.indeed (job_id, source);
END
GO

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
