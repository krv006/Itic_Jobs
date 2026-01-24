import os
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

load_dotenv()

CBU_URL = "https://cbu.uz/oz/arkhiv-kursov-valyut/json/"
TABLE_NAME = "cbu_currency_rates"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def to_decimal_safe(value):
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    s = str(value).strip().replace(" ", "").replace(",", ".")
    if s == "":
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d:
            return d[k]
        for kk in d.keys():
            if str(kk).lower() == str(k).lower():
                return d[kk]
    return default


def parse_date(date_str: str):
    ds = str(date_str).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(ds, fmt).date()
        except ValueError:
            pass
    return None


def normalize_item(item: dict):
    ccy = pick(item, "Ccy", "ccy")
    if not ccy:
        return None

    date_str = pick(item, "Date", "date")
    rate_date = parse_date(date_str)
    if not rate_date:
        raise ValueError(f"Could not parse Date='{date_str}' for ccy={ccy}")

    nominal = to_decimal_safe(pick(item, "Nominal", "nominal"))
    rate = to_decimal_safe(pick(item, "Rate", "rate"))
    diff = to_decimal_safe(pick(item, "Diff", "diff"))

    # UZS per 1 unit (nominal hisobga olinadi)
    uzs_per_1 = None
    if rate is not None and nominal not in (None, Decimal("0")):
        uzs_per_1 = rate / nominal

    return {
        "ccy": str(ccy).upper(),
        "code": pick(item, "Code", "code"),
        "ccy_nm_uz": pick(item, "CcyNm_UZ", "CcyNm_Uz", "ccy_nm_uz"),
        "ccy_nm_ru": pick(item, "CcyNm_RU", "CcyNm_Ru", "ccy_nm_ru"),
        "ccy_nm_en": pick(item, "CcyNm_EN", "CcyNm_En", "ccy_nm_en"),
        "nominal": nominal,
        "rate": rate,             # original Rate (UZS for Nominal units)
        "diff": diff,
        "rate_date": rate_date,
        "uzs_per_1": uzs_per_1,   # computed
    }


def ensure_table_and_columns(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id BIGSERIAL PRIMARY KEY,
        ccy TEXT NOT NULL,
        code TEXT NULL,
        ccy_nm_uz TEXT NULL,
        ccy_nm_ru TEXT NULL,
        ccy_nm_en TEXT NULL,
        nominal NUMERIC(18,6) NULL,
        rate NUMERIC(18,6) NULL,
        diff NUMERIC(18,6) NULL,
        rate_date DATE NOT NULL,
        retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source TEXT NOT NULL DEFAULT 'cbu.uz'
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_NAME}_ccy_date
        ON {TABLE_NAME} (ccy, rate_date);

    CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_rate_date
        ON {TABLE_NAME} (rate_date);

    CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_ccy
        ON {TABLE_NAME} (ccy);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

        # ✅ New column: dollorga_nisbati (USD per 1 unit of currency)
        cur.execute(f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMN IF NOT EXISTS dollorga_nisbati NUMERIC(24,12) NULL;
        """)

    conn.commit()


def fetch_cbu():
    logging.info("Fetching CBU JSON...")
    r = requests.get(CBU_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected JSON format: expected list")
    logging.info("Fetched %s rows", len(data))
    return data


def upsert_rates(conn, items):
    now = datetime.now(timezone.utc)

    normalized = []
    for it in items:
        n = normalize_item(it)
        if n:
            normalized.append(n)

    if not normalized:
        logging.warning("No rows after normalization.")
        return

    # ✅ Find USD UZS-per-1 for the date
    usd_uzs_per_1 = None
    for n in normalized:
        if n["ccy"] == "USD":
            usd_uzs_per_1 = n["uzs_per_1"]
            break

    if usd_uzs_per_1 in (None, Decimal("0")):
        raise RuntimeError("USD rate not found in CBU response (cannot compute dollar-relative rates).")

    values = []
    for n in normalized:
        uzs_per_1 = n["uzs_per_1"]
        dollorga_nisbati = None

        # ✅ USD_per_currency = (UZS_per_1_currency) / (UZS_per_1_USD)
        if uzs_per_1 is not None:
            dollorga_nisbati = uzs_per_1 / usd_uzs_per_1

        # For USD itself -> 1
        if n["ccy"] == "USD":
            dollorga_nisbati = Decimal("1")

        values.append((
            n["ccy"],
            n["code"],
            n["ccy_nm_uz"],
            n["ccy_nm_ru"],
            n["ccy_nm_en"],
            n["nominal"],
            n["rate"],
            n["diff"],
            n["rate_date"],
            dollorga_nisbati,
            now,
            "cbu.uz",
        ))

    sql = f"""
    INSERT INTO {TABLE_NAME} (
        ccy, code, ccy_nm_uz, ccy_nm_ru, ccy_nm_en,
        nominal, rate, diff, rate_date,
        dollorga_nisbati,
     retrieved_at, source
    )
    VALUES %s
    ON CONFLICT (ccy, rate_date)
    DO UPDATE SET
        code = EXCLUDED.code,
        ccy_nm_uz = EXCLUDED.ccy_nm_uz,
        ccy_nm_ru = EXCLUDED.ccy_nm_ru,
        ccy_nm_en = EXCLUDED.ccy_nm_en,
        nominal = EXCLUDED.nominal,
        rate = EXCLUDED.rate,
        diff = EXCLUDED.diff,
        dollorga_nisbati = EXCLUDED.dollorga_nisbati,
        retrieved_at = EXCLUDED.retrieved_at,
        source = EXCLUDED.source;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    logging.info("Upsert done. Rows processed: %s", len(values))


def main():
    conn = psycopg2.connect(
        host=env_required("DB_HOST"),
        port=int(env_required("DB_PORT")),
        dbname=env_required("DB_NAME"),
        user=env_required("DB_USER"),
        password=env_required("DB_PASSWORD"),
    )

    try:
        ensure_table_and_columns(conn)
        data = fetch_cbu()
        upsert_rates(conn, data)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
