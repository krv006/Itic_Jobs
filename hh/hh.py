import datetime
import json
import os
import re
import time
from typing import Optional, Tuple, Set, Dict, List
from urllib.parse import urlparse, parse_qs, unquote_plus, quote_plus

import geonamescache
import psycopg2
import pycountry
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    NoSuchWindowException,
    WebDriverException,
    InvalidSessionIdException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


load_dotenv()


# ============================================================
# CONFIG
# ============================================================
TABLE_NAME = os.getenv("HH_TABLE_NAME", "public.hh")

DEFAULT_WAIT = int(os.getenv("HH_DEFAULT_WAIT", "5"))
HEADLESS = os.getenv("HEADLESS", "false").strip().lower() == "true"

HH_MAX_PAGES_PER_KEYWORD = int(os.getenv("HH_MAX_PAGES_PER_KEYWORD", "10"))
HH_PAGE_SLEEP = float(os.getenv("HH_PAGE_SLEEP", "0.7"))
HH_VACANCY_SLEEP = float(os.getenv("HH_VACANCY_SLEEP", "0.2"))

CHROME_VERSION_MAIN = os.getenv("CHROME_VERSION_MAIN", "").strip()

HH_BASE_SEARCH_URL = "https://tashkent.hh.uz/search/vacancy"


# ============================================================
# FALLBACK ENGLISH NORMALIZER
# ============================================================
_RU2LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "yo", "ж": "j", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "x", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "shch", "ъ": "", "ы": "i", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}

_MAP_EXACT = {
    "ташкент": "Tashkent",
    "тoшкент": "Tashkent",
    "toshkent": "Tashkent",
    "минск": "Minsk",
    "москва": "Moscow",
    "астана": "Astana",
    "алматы": "Almaty",
    "лимасол": "Limassol",
    "киев": "Kyiv",
    "санкт-петербург": "Saint Petersburg",

    "графический дизайнер": "Graphic Designer",
    "дизайнер": "Designer",
    "аниматор": "Animator",
    "анимация": "Animation",
    "моушн-дизайн": "Motion Design",
    "веб-дизайн": "Web Design",

    "за месяц": "per month",
    "на руки": "net",
    "от": "from",
    "до": "up to",
}

_MAP_IN_TEXT = {
    "за месяц": "per month",
    "на руки": "net",
    "до вычета налогов": "gross",
    "после вычета налогов": "net",
    "обучение и развитие": "Training & Development",
    "аналитическое мышление": "Analytical Thinking",
    "управление командой": "Team Management",
    "планирование ресурсов": "Resource Planning",
    "разработка брендбука": "Brandbook Development",
    "разработка логотипов": "Logo Design",
    "визуализация данных": "Data Visualization",
    "стилизация изображений": "Image Styling",
    "моушн-дизайн": "Motion Design",
    "веб-дизайн": "Web Design",
    "удаленно": "Remote",
    "удалённо": "Remote",
    "полная занятость": "Full Time",
    "частичная занятость": "Part Time",
}

_LANG_MAP = {
    "русский": "Russian",
    "английский": "English",
    "казахский": "Kazakh",
    "узбекский": "Uzbek",
}

_PROF_MAP = {
    "начальный": "Beginner",
    "средне-продвинутый": "Upper-Intermediate",
    "продвинутый": "Advanced",
    "свободно": "Fluent",
    "родной": "Native",
}

_TEXT_CACHE: dict[str, str] = {}


def _has_cyrillic(text: str) -> bool:
    return bool(text) and bool(re.search(r"[А-Яа-яЁё]", text))


def _translit_ru_to_lat(text: str) -> str:
    out = []

    for ch in text:
        low = ch.lower()

        if low in _RU2LAT:
            t = _RU2LAT[low]

            if ch.isupper() and t:
                t = t[0].upper() + t[1:]

            out.append(t)
        else:
            out.append(ch)

    return "".join(out)


def to_english(text: str) -> str:
    if not text:
        return ""

    s = re.sub(r"\s+", " ", text.strip())

    if not s:
        return ""

    if s in _TEXT_CACHE:
        return _TEXT_CACHE[s]

    low = s.lower()

    if low in _MAP_EXACT:
        res = _MAP_EXACT[low]
        _TEXT_CACHE[s] = res
        return res

    m = re.match(
        r"^\s*([А-Яа-яЁё]+)\s*[—-]\s*(A1|A2|B1|B2|C1|C2)\s*[—-]\s*([А-Яа-яЁё\- ]+)\s*$",
        s,
    )

    if m:
        lang_ru = m.group(1).strip().lower()
        level = m.group(2).strip()
        prof_ru = m.group(3).strip().lower()

        lang_en = _LANG_MAP.get(lang_ru, _translit_ru_to_lat(m.group(1).strip()))
        prof_en = _PROF_MAP.get(prof_ru, _translit_ru_to_lat(m.group(3).strip()))

        res = f"{lang_en} - {level} - {prof_en}"
        _TEXT_CACHE[s] = res
        return res

    res = s

    for ru, en in _MAP_IN_TEXT.items():
        res = re.sub(re.escape(ru), en, res, flags=re.IGNORECASE)

    for ru, en in _LANG_MAP.items():
        res = re.sub(rf"\b{re.escape(ru)}\b", en, res, flags=re.IGNORECASE)

    for ru, en in _PROF_MAP.items():
        res = re.sub(rf"\b{re.escape(ru)}\b", en, res, flags=re.IGNORECASE)

    if _has_cyrillic(res):
        res = _translit_ru_to_lat(res)

    res = re.sub(r"\s+", " ", res).strip()
    _TEXT_CACHE[s] = res

    return res


def normalize_skills_csv(skills_csv: str) -> str:
    if not skills_csv:
        return ""

    items = [x.strip() for x in skills_csv.split(",") if x.strip()]
    out = []
    seen = set()

    for item in items:
        en = to_english(item)

        if en and en not in seen:
            out.append(en)
            seen.add(en)

    return ",".join(out)


def normalize_salary_range(s: str) -> str:
    if not s:
        return ""

    raw = re.sub(r"\s+", " ", s.strip())

    if not raw:
        return ""

    t = raw.lower()
    t = t.replace("from", "ot")
    t = t.replace("up to", "do")
    t = t.replace("до вычета налогов", "")
    t = t.replace("на руки", "")

    cur = ""

    cur_m = re.search(
        r"(?i)\b(usd|eur|gbp|kzt|rub|uah|byn|br|pln|try|aed|sar|cad|aud|chf|sek|nok|dkk|uzs)\b",
        raw,
    )

    if cur_m:
        cur = cur_m.group(1).upper()
    else:
        sym_m = re.search(r"[\$€£₽₸]", raw)

        if sym_m:
            cur = sym_m.group(0)

    def _num_after(keyword: str) -> Optional[str]:
        m = re.search(rf"\b{keyword}\b\s*([\d\s]+)", t)

        if not m:
            return None

        n = re.sub(r"[^\d\s]", "", m.group(1))
        n = re.sub(r"\s+", " ", n).strip()

        return n or None

    frm = _num_after("ot")
    to = _num_after("do")

    if frm and to:
        out = f"{frm} - {to}"
    elif frm and not to:
        out = f"{frm} -"
    elif to and not frm:
        out = f"- {to}"
    else:
        nums = re.findall(r"\d[\d\s]*\d|\d+", t)
        nums = [re.sub(r"[^\d\s]", "", n) for n in nums]
        nums = [re.sub(r"\s+", " ", n).strip() for n in nums if n.strip()]

        if not nums:
            return ""

        out = f"{nums[0]} - {nums[1]}" if len(nums) >= 2 else nums[0]

    return f"{out} {cur}".strip() if cur else out


# ============================================================
# COUNTRY RESOLVER
# ============================================================
gc = geonamescache.GeonamesCache()
_CITIES = gc.get_cities()
_COUNTRIES = gc.get_countries()

_CITY_TO_CC: Dict[str, Set[str]] = {}

for _id, c in _CITIES.items():
    name = (c.get("name") or "").strip().lower()
    cc = (c.get("countrycode") or "").strip().upper()

    if not name or not cc:
        continue

    _CITY_TO_CC.setdefault(name, set()).add(cc)

EXTRA_CITY_ALIASES = {
    "sf": "san francisco",
    "nyc": "new york",
    "la": "los angeles",
    "dc": "washington",
    "tashkent": "tashkent",
    "toshkent": "tashkent",
}

for k, v in EXTRA_CITY_ALIASES.items():
    if v in _CITY_TO_CC:
        _CITY_TO_CC.setdefault(k, set()).update(_CITY_TO_CC[v])

US_STATE_ABBR = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in",
    "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne",
    "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
}

CA_PROVINCES = {
    "ontario",
    "quebec",
    "british columbia",
    "alberta",
    "manitoba",
    "saskatchewan",
    "nova scotia",
    "new brunswick",
    "newfoundland and labrador",
    "prince edward island",
    "northwest territories",
    "nunavut",
    "yukon",
}

_LOCATION_CACHE: Dict[str, Tuple[Optional[str], Optional[str]]] = {}


def _cc_to_country_name(cc: str) -> Optional[str]:
    cc = (cc or "").upper().strip()

    if not cc:
        return None

    info = _COUNTRIES.get(cc)

    if info and info.get("name"):
        return info["name"]

    obj = pycountry.countries.get(alpha_2=cc)

    return obj.name if obj else None


def _country_code_from_text(token: str) -> Optional[str]:
    t = (token or "").strip()

    if not t:
        return None

    low = t.lower()

    if low in {"us", "u.s.", "usa", "united states", "united states of america"}:
        return "US"

    if low in {"uk", "u.k.", "united kingdom", "britain", "england"}:
        return "GB"

    if low in {"uae", "united arab emirates"}:
        return "AE"

    if low in {"uzbekistan", "uzbekistan republic", "o'zbekiston", "uz"}:
        return "UZ"

    if low in {"kazakhstan", "kz"}:
        return "KZ"

    if low in {"russia", "russian federation", "ru"}:
        return "RU"

    if re.fullmatch(r"[a-z]{2}", low):
        cc = low.upper()

        if pycountry.countries.get(alpha_2=cc):
            return cc

        return None

    if re.fullmatch(r"[a-z]{3}", low):
        obj = pycountry.countries.get(alpha_3=low.upper())

        return obj.alpha_2 if obj else None

    try:
        matches = pycountry.countries.search_fuzzy(t)

        if matches:
            return matches[0].alpha_2

    except Exception:
        pass

    return None


def extract_country_name_and_code_from_location(location: str) -> Tuple[Optional[str], Optional[str]]:
    if not location:
        return None, None

    s = location.strip()

    if not s:
        return None, None

    key = s.lower()

    if key in _LOCATION_CACHE:
        return _LOCATION_CACHE[key]

    low = key
    low = low.replace("&", " and ")
    low = re.sub(r"\bor\b", " ", low)
    low = re.sub(r"\s+", " ", low).strip()

    chunks = re.split(r"[;|/]", low)
    chunks = [c.strip() for c in chunks if c.strip()]

    found_cc: Set[str] = set()

    for chunk in chunks:
        parts = [p.strip() for p in chunk.split(",") if p.strip()]
        joined = " ".join(parts)

        for p in parts:
            cc = _country_code_from_text(p)

            if cc:
                found_cc.add(cc)

        cc2 = _country_code_from_text(joined)

        if cc2:
            found_cc.add(cc2)

        for p in parts:
            p2 = p.lower()

            if p2 in US_STATE_ABBR:
                found_cc.add("US")

            if p2 in CA_PROVINCES:
                found_cc.add("CA")

        if parts:
            city = parts[0].lower().strip()
            city = EXTRA_CITY_ALIASES.get(city, city)

            if city in _CITY_TO_CC:
                for cc in _CITY_TO_CC[city]:
                    found_cc.add(cc)

        c2 = chunk.lower().strip()
        c2 = EXTRA_CITY_ALIASES.get(c2, c2)

        if c2 in _CITY_TO_CC:
            for cc in _CITY_TO_CC[c2]:
                found_cc.add(cc)

    if not found_cc:
        res = (None, None)
        _LOCATION_CACHE[key] = res
        return res

    names: List[str] = []
    codes: List[str] = []

    for cc in sorted(found_cc):
        cname = _cc_to_country_name(cc)

        if cname and cname not in names:
            names.append(cname)

        if cc not in codes:
            codes.append(cc)

    res = ("; ".join(names), "; ".join(codes))
    _LOCATION_CACHE[key] = res

    return res


# ============================================================
# DB
# ============================================================
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    conn.autocommit = True
    return conn


def create_table_if_not_exists(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            job_title TEXT,
            location TEXT,
            country TEXT,
            country_code TEXT,
            skills TEXT,
            salary TEXT,
            education TEXT,
            job_type TEXT,
            company_name TEXT,
            job_url TEXT,
            source TEXT,
            posted_date DATE,
            created_at TIMESTAMP DEFAULT NOW(),
            job_subtitle TEXT,
            search_query TEXT
        );
        """
    )

    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS id BIGSERIAL;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS job_subtitle TEXT;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS search_query TEXT;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS country TEXT;")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS country_code TEXT;")

    cursor.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = split_part('{TABLE_NAME}', '.', 1)
                  AND tablename  = split_part('{TABLE_NAME}', '.', 2)
                  AND indexname  = 'hh_job_id_unique'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX hh_job_id_unique ON {TABLE_NAME} (job_id)';
            END IF;
        END$$;
        """
    )

    print("[DB] hh table ready ✅")


def save_to_database(cursor, data: dict) -> bool:
    try:
        cursor.execute(
            f"""
            INSERT INTO {TABLE_NAME} (
                job_id,
                job_title,
                location,
                country,
                country_code,
                skills,
                salary,
                education,
                job_type,
                company_name,
                job_url,
                source,
                posted_date,
                job_subtitle,
                search_query
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (job_id) DO NOTHING;
            """,
            (
                data["job_id"],
                data["job_title"],
                data["location"],
                data.get("country"),
                data.get("country_code"),
                data["skills"],
                data["salary"],
                data["education"],
                data["job_type"],
                data["company_name"],
                data["job_url"],
                data["source"],
                data["posted_date"],
                data["job_subtitle"],
                data["search_query"],
            ),
        )

        return cursor.rowcount == 1

    except Exception as e:
        print(f"❌ DB ERROR: {type(e).__name__}: {e}")
        return False


# ============================================================
# DRIVER SAFE LAYER
# ============================================================
def create_driver():
    options = uc.ChromeOptions()

    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-allow-origins=*")

    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")

    options.page_load_strategy = "eager"

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
    }
    options.add_experimental_option("prefs", prefs)

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    )

    try:
        if CHROME_VERSION_MAIN:
            driver = uc.Chrome(options=options, version_main=int(CHROME_VERSION_MAIN))
        else:
            driver = uc.Chrome(options=options)
    except Exception as e:
        print(f"[DRIVER CREATE WARN] version_main failed: {type(e).__name__}: {e}")
        driver = uc.Chrome(options=options)

    driver.set_page_load_timeout(25)
    driver.implicitly_wait(0)

    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd(
            "Network.setBlockedURLs",
            {
                "urls": [
                    "*.png",
                    "*.jpg",
                    "*.jpeg",
                    "*.gif",
                    "*.webp",
                    "*.svg",
                    "*.css",
                    "*.woff",
                    "*.woff2",
                    "*.ttf",
                    "*google-analytics*",
                    "*yandex*",
                    "*doubleclick*",
                    "*adservice*",
                    "*ads*",
                    "*analytics*",
                    "*counter*",
                    "*metrics*",
                ]
            },
        )
    except Exception:
        pass

    return driver


def safe_quit_driver(driver):
    try:
        if driver:
            driver.quit()
    except Exception:
        pass


def restart_driver(driver):
    print("[DRIVER] restarting Chrome...")
    safe_quit_driver(driver)
    time.sleep(1)

    driver = create_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)

    return driver, wait


def safe_get(driver, wait, url: str, retries: int = 2):
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            return driver, wait, True

        except (NoSuchWindowException, InvalidSessionIdException, WebDriverException) as e:
            last_err = e
            print(f"[DRIVER GET DEAD] attempt={attempt}/{retries}")
            print(f"[URL] {url}")
            print(f"[ERR] {type(e).__name__}: {str(e)[:200]}")
            driver, wait = restart_driver(driver)

        except Exception as e:
            last_err = e
            print(f"[DRIVER GET ERROR] attempt={attempt}/{retries}")
            print(f"[URL] {url}")
            print(f"[ERR] {type(e).__name__}: {str(e)[:200]}")
            time.sleep(1)

    print(f"[SAFE_GET FAILED] url={url} err={last_err}")
    return driver, wait, False


def safe_wait_presence(driver, wait, locator, label: str, retries: int = 1) -> bool:
    for attempt in range(1, retries + 1):
        try:
            wait.until(EC.presence_of_element_located(locator))
            return True

        except TimeoutException:
            print(f"[WAIT TIMEOUT] {label} attempt={attempt}/{retries}")

        except (NoSuchWindowException, InvalidSessionIdException, WebDriverException) as e:
            print(f"[WAIT DRIVER DEAD] {label} attempt={attempt}/{retries}")
            print(f"[ERR] {type(e).__name__}: {str(e)[:200]}")
            return False

    return False


def safe_find_elements(driver, by, value) -> list:
    try:
        return driver.find_elements(by, value)
    except (NoSuchWindowException, InvalidSessionIdException, WebDriverException):
        return []
    except Exception:
        return []


def safe_page_source(driver) -> str:
    try:
        return driver.page_source or ""
    except (NoSuchWindowException, InvalidSessionIdException, WebDriverException):
        return ""
    except Exception:
        return ""


def safe_text(driver, xpath: str) -> str:
    """
    MAX speed: element topilmasa kutmaydi.
    Faqat detail page yuklangandan keyin fieldlarni tez oladi.
    """
    try:
        elements = driver.find_elements(By.XPATH, xpath)

        if not elements:
            return ""

        return elements[0].text.strip()

    except (NoSuchWindowException, InvalidSessionIdException, WebDriverException):
        return ""
    except Exception:
        return ""


def safe_css_texts(driver, selector: str) -> list[str]:
    out = []

    for el in safe_find_elements(driver, By.CSS_SELECTOR, selector):
        try:
            txt = el.text.strip()

            if txt:
                out.append(txt)
        except Exception:
            pass

    return out


# ============================================================
# VALIDATION
# ============================================================
def is_valid_job_id(job_id: str) -> bool:
    return bool(job_id) and job_id.isdigit() and len(job_id) >= 6


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

    t = title.lower()

    return not any(bad in t for bad in bad_words)


def extract_search_query_from_url(url: str) -> str:
    try:
        qs = parse_qs(urlparse(url).query)
        val = (qs.get("query") or qs.get("text") or [""])[0]
        return unquote_plus(val).strip()
    except Exception:
        return ""


# ============================================================
# POSTED DATE
# ============================================================
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


def parse_posted_date_from_text(text: str) -> Optional[datetime.date]:
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
        return today - datetime.timedelta(days=int(m.group(1)))

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
    Fast posted date:
    Avval HTML regex, keyin tezkor XPath. Hech qachon uzun kutmaydi.
    """
    html = safe_page_source(driver)

    candidates = []

    if html:
        patterns = [
            r"(Вакансия опубликована[^<]{0,120})",
            r"(Опубликовано[^<]{0,120})",
            r"(сегодня)",
            r"(вчера)",
            r"(\d+\s*(?:дня|дней|день)\s*назад)",
            r"(\d{1,2}\s+[а-яё]+\s+\d{4})",
        ]

        for pattern in patterns:
            m = re.search(pattern, html, flags=re.IGNORECASE)

            if m:
                candidates.append(m.group(1))

    xpaths = [
        "//*[@data-qa='vacancy-view-creation-time']",
        "//*[contains(text(),'Вакансия опубликована')]",
        "//*[contains(text(),'Опубликовано')]",
    ]

    for xp in xpaths:
        txt = safe_text(driver, xp)

        if txt:
            candidates.append(txt)

    for candidate in candidates:
        parsed_date = parse_posted_date_from_text(candidate)

        if parsed_date:
            return parsed_date

    return datetime.date.today()


# ============================================================
# SEARCH RESULT URLS
# ============================================================
def get_search_result_urls(driver, wait) -> List[str]:
    ok = safe_wait_presence(
        driver,
        wait,
        (By.TAG_NAME, "body"),
        "search body",
        retries=1,
    )

    if not ok:
        return []

    try:
        WebDriverWait(driver, 4).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-qa='serp-item__title']"))
        )
    except TimeoutException:
        html = safe_page_source(driver)

        if "captcha" in html.lower() or "подтвердите" in html.lower():
            print("❌ HH CAPTCHA / BLOCK detected")
        else:
            print("❌ NO RESULT LINKS")

        return []
    except (NoSuchWindowException, InvalidSessionIdException, WebDriverException):
        return []

    links = safe_find_elements(driver, By.CSS_SELECTOR, "a[data-qa='serp-item__title']")

    urls = []
    seen = set()

    for a in links:
        try:
            href = a.get_attribute("href")
        except Exception:
            continue

        if not href or "/vacancy/" not in href:
            continue

        job_id = href.split("/vacancy/")[-1].split("?")[0].strip()

        if not job_id.isdigit():
            continue

        if href not in seen:
            seen.add(href)
            urls.append(href)

    return urls


# ============================================================
# PARSE VACANCY
# ============================================================
def parse_vacancy_page(driver, vacancy_url: str, keyword: str) -> Optional[dict]:
    job_id = vacancy_url.split("/vacancy/")[-1].split("?")[0].strip()

    if not is_valid_job_id(job_id):
        print(f"[SKIP] invalid job_id={job_id}")
        return None

    raw_title = safe_text(driver, "//h1")

    if not is_valid_job_title(raw_title):
        print(f"[SKIP] invalid title={raw_title}")
        return None

    posted_date = get_hh_posted_date(driver)

    raw_location = safe_text(driver, "//span[@data-qa='vacancy-view-raw-address']")

    if not raw_location:
        raw_location = safe_text(driver, "//*[@data-qa='vacancy-view-location']")

    raw_skills = safe_text(driver, "//ul[contains(@class,'vacancy-skill-list')]").replace("\n", ",")

    if not raw_skills:
        skills = safe_css_texts(driver, "[data-qa='bloko-tag__text']")
        raw_skills = ",".join(skills)

    raw_salary = safe_text(driver, "//*[@data-qa='vacancy-salary']")

    if not raw_salary:
        raw_salary = safe_text(driver, "//span[contains(@data-qa,'vacancy-salary')]")

    job_type_parts = []

    for xp in [
        "//*[@data-qa='vacancy-experience']",
        "//*[@data-qa='vacancy-view-employment-mode']",
        "//*[@data-qa='vacancy-view-employment']",
        "//*[@data-qa='vacancy-view-schedule']",
        "//div[@data-qa='vacancy-working-hours']",
    ]:
        txt = safe_text(driver, xp)

        if txt:
            job_type_parts.append(txt)

    raw_job_type = " | ".join(job_type_parts)

    raw_company = safe_text(driver, "//*[@data-qa='vacancy-company-name']")

    if not raw_company:
        raw_company = safe_text(driver, "//div[@data-qa='vacancy-company__details']")

    location = to_english(raw_location)
    country, country_code = extract_country_name_and_code_from_location(location)

    data = {
        "job_id": job_id,
        "job_title": to_english(raw_title),
        "location": location,
        "country": country,
        "country_code": country_code,
        "skills": normalize_skills_csv(raw_skills),
        "salary": normalize_salary_range(to_english(raw_salary)),
        "education": "",
        "job_type": to_english(raw_job_type),
        "company_name": to_english(raw_company),
        "job_url": vacancy_url,
        "source": "hh.uz",
        "posted_date": posted_date,
        "job_subtitle": str(keyword),
        "search_query": str(keyword),
    }

    return data


# ============================================================
# SCRAPER
# ============================================================
def get_hh_vacancies(jobs_list):
    conn = get_db_connection()
    cursor = conn.cursor()

    create_table_if_not_exists(cursor)

    driver = None
    wait = None

    inserted = 0
    duplicates = 0
    parse_failed = 0
    pages_scanned = 0
    scanned_vacancies = 0
    driver_restarts = 0

    try:
        driver = create_driver()
        wait = WebDriverWait(driver, DEFAULT_WAIT)

        for job in jobs_list:
            page = 0

            while True:
                q = quote_plus(str(job))
                search_url = f"{HH_BASE_SEARCH_URL}?text={q}&page={page}"

                print("\n==============================")
                print(f"[SEARCH] job={job} page={page}")
                print(f"[URL] {search_url}")

                driver, wait, ok = safe_get(driver, wait, search_url)

                if not ok:
                    print(f"[SKIP PAGE] cannot open search page: {search_url}")
                    driver, wait = restart_driver(driver)
                    driver_restarts += 1
                    break

                time.sleep(HH_PAGE_SLEEP)

                urls = get_search_result_urls(driver, wait)

                if not urls:
                    print(f"[STOP] no vacancies found for job={job}, page={page}")
                    break

                pages_scanned += 1
                print(f"[FOUND] urls={len(urls)}")

                for idx, vacancy_url in enumerate(urls, start=1):
                    try:
                        print(f"\n[VACANCY] {idx}/{len(urls)} {vacancy_url}")

                        driver, wait, ok = safe_get(driver, wait, vacancy_url)

                        if not ok:
                            parse_failed += 1
                            print(f"[SKIP VACANCY] cannot open: {vacancy_url}")
                            driver, wait = restart_driver(driver)
                            driver_restarts += 1
                            continue

                        h1_ok = safe_wait_presence(
                            driver,
                            wait,
                            (By.XPATH, "//h1"),
                            "vacancy h1",
                            retries=1,
                        )

                        if not h1_ok:
                            parse_failed += 1
                            print(f"[SKIP VACANCY] h1 not loaded: {vacancy_url}")
                            driver, wait = restart_driver(driver)
                            driver_restarts += 1
                            continue

                        data = parse_vacancy_page(driver, vacancy_url, str(job))

                        if not data:
                            parse_failed += 1
                            continue

                        scanned_vacancies += 1
                        saved = save_to_database(cursor, data)

                        if saved:
                            inserted += 1
                            print(
                                f"✅ SAVED {data['job_id']} | "
                                f"{data['job_title']} | "
                                f"salary={data['salary']} | "
                                f"country={data.get('country_code')}"
                            )
                        else:
                            duplicates += 1
                            print(f"⚠️ DUPLICATE {data['job_id']} | {data['job_title']}")

                        time.sleep(HH_VACANCY_SLEEP)

                    except (NoSuchWindowException, InvalidSessionIdException, WebDriverException) as e:
                        parse_failed += 1
                        print(f"[VACANCY DRIVER ERROR] {type(e).__name__}: {str(e)[:200]}")
                        driver, wait = restart_driver(driver)
                        driver_restarts += 1
                        continue

                    except Exception as e:
                        parse_failed += 1
                        print(f"❌ PARSE ERROR: {type(e).__name__}: {e}")
                        continue

                page += 1

                if page >= HH_MAX_PAGES_PER_KEYWORD:
                    print(f"[STOP] max pages reached for keyword={job}: {HH_MAX_PAGES_PER_KEYWORD}")
                    break

    finally:
        safe_quit_driver(driver)

        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

        print("\nDONE ✅")
        print(f"inserted={inserted}")
        print(f"duplicates={duplicates}")
        print(f"parse_failed={parse_failed}")
        print(f"pages_scanned={pages_scanned}")
        print(f"scanned_vacancies={scanned_vacancies}")
        print(f"driver_restarts={driver_restarts}")


# ============================================================
# JOB LIST
# ============================================================
def load_jobs(path: str = "job_list.json") -> list[str]:
    if not os.path.exists(path):
        print(f"[WARN] {path} not found")
        return []

    with open(path, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    if isinstance(jobs, dict):
        for key in ("jobs", "keywords", "list"):
            if key in jobs and isinstance(jobs[key], list):
                jobs = jobs[key]
                break

    if not isinstance(jobs, list):
        print("[WARN] job_list.json must be list or dict with jobs/keywords/list")
        return []

    result = []

    for item in jobs:
        value = str(item).strip()

        if value:
            result.append(value)

    return result


def main():
    jobs = load_jobs("job_list.json")

    if not jobs:
        print("[STOP] no jobs loaded from job_list.json")
        return

    print(f"[KEYWORDS] loaded={len(jobs)}")

    get_hh_vacancies(jobs)

    print("DONE")


if __name__ == "__main__":
    main()

