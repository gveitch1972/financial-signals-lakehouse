from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.config import BRONZE_FX_RAW

FX_API_BASE_URL = os.getenv("FX_API_BASE_URL", "https://api.frankfurter.app")
DEFAULT_BASE_CURRENCY = "GBP"
DEFAULT_QUOTE_CURRENCIES = ["USD", "EUR", "JPY", "CHF"]
DEFAULT_START_DATE = "2020-01-01"
HTTP_HEADERS = {
    "User-Agent": "financial-signals-lakehouse/1.0",
    "Accept": "application/json",
}

FX_SCHEMA = StructType([
    StructField("base_currency", StringType(), False),
    StructField("quote_currency", StringType(), False),
    StructField("rate", DoubleType(), True),
    StructField("rate_timestamp", TimestampType(), True),
    StructField("source_name", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
    StructField("run_id", StringType(), False),
])


def parse_csv_env(name, default_values):
    raw_value = os.getenv(name)
    if not raw_value:
        return list(default_values)

    parsed = []
    for chunk in raw_value.replace(";", ",").split(","):
        value = chunk.strip().upper()
        if value:
            parsed.append(value)
    return parsed or list(default_values)


def get_load_mode():
    load_mode = os.getenv("LOAD_MODE", "snapshot").strip().lower()
    if load_mode not in {"snapshot", "backfill"}:
        raise ValueError(f"Unsupported LOAD_MODE '{load_mode}'. Expected 'snapshot' or 'backfill'.")
    return load_mode


def get_date_range():
    start_date = os.getenv("START_DATE", DEFAULT_START_DATE)
    end_date = os.getenv("END_DATE", date.today().isoformat())
    if start_date > end_date:
        raise ValueError(f"START_DATE {start_date} must be on or before END_DATE {end_date}.")
    return start_date, end_date


def build_request_url(load_mode, base_currency, quote_currencies, start_date, end_date):
    query = urlencode({"from": base_currency, "to": ",".join(quote_currencies)})
    if load_mode == "backfill":
        return f"{FX_API_BASE_URL}/{start_date}..{end_date}?{query}"
    return f"{FX_API_BASE_URL}/latest?{query}"


def fetch_fx_payload(url: str) -> dict:
    request = Request(url, headers=HTTP_HEADERS)
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def timestamp_from_date(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


def normalise_snapshot_payload(payload: dict, run_id: str, source_url: str) -> list[Row]:
    base = str(payload.get("base"))
    rate_timestamp = timestamp_from_date(payload["date"])
    ingested_at = datetime.now(timezone.utc)

    rows = []
    for quote_currency, rate in (payload.get("rates") or {}).items():
        rows.append(
            Row(
                base_currency=base,
                quote_currency=str(quote_currency),
                rate=float(rate) if rate is not None else None,
                rate_timestamp=rate_timestamp,
                source_name="frankfurter",
                source_url=source_url,
                ingested_at=ingested_at,
                run_id=run_id,
            )
        )
    return rows


def normalise_backfill_payload(payload: dict, run_id: str, source_url: str) -> list[Row]:
    base = str(payload.get("base"))
    ingested_at = datetime.now(timezone.utc)
    rows = []

    for rate_date, values in sorted((payload.get("rates") or {}).items()):
        rate_timestamp = timestamp_from_date(rate_date)
        for quote_currency, rate in (values or {}).items():
            rows.append(
                Row(
                    base_currency=base,
                    quote_currency=str(quote_currency),
                    rate=float(rate) if rate is not None else None,
                    rate_timestamp=rate_timestamp,
                    source_name="frankfurter",
                    source_url=source_url,
                    ingested_at=ingested_at,
                    run_id=run_id,
                )
            )
    return rows


def ensure_table_exists(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_FX_RAW} (
            base_currency STRING NOT NULL,
            quote_currency STRING NOT NULL,
            rate DOUBLE,
            rate_timestamp TIMESTAMP,
            source_name STRING,
            source_url STRING,
            ingested_at TIMESTAMP NOT NULL,
            run_id STRING NOT NULL
        )
        USING DELTA
    """)


def main() -> None:
    spark = SparkSession.builder.appName("bronze-fx-ingestion").getOrCreate()
    run_id = str(uuid.uuid4())
    load_mode = get_load_mode()
    start_date, end_date = get_date_range()
    base_currency = os.getenv("FX_BASE_CURRENCY", DEFAULT_BASE_CURRENCY).strip().upper()
    quote_currencies = parse_csv_env("FX_QUOTE_CURRENCIES", DEFAULT_QUOTE_CURRENCIES)
    request_url = build_request_url(load_mode, base_currency, quote_currencies, start_date, end_date)

    payload = fetch_fx_payload(request_url)
    rows = (
        normalise_backfill_payload(payload, run_id, request_url)
        if load_mode == "backfill"
        else normalise_snapshot_payload(payload, run_id, request_url)
    )

    if not rows:
        raise RuntimeError("FX ingestion returned zero rows.")

    df = spark.createDataFrame(rows, schema=FX_SCHEMA)
    ensure_table_exists(spark)

    (
        df.select(
            "base_currency",
            "quote_currency",
            "rate",
            "rate_timestamp",
            "source_name",
            "source_url",
            "ingested_at",
            "run_id",
        )
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_FX_RAW)
    )

    print(
        f"Wrote {df.count()} rows to {BRONZE_FX_RAW} "
        f"with run_id={run_id}, mode={load_mode}, base={base_currency}, quotes={','.join(quote_currencies)}"
    )


if __name__ == "__main__":
    main()
