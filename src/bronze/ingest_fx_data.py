from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

from src.common.config import BRONZE_FX_RAW


CATALOG = "fin_signals_dev"
BRONZE_SCHEMA = "bronze"
TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw"

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

# Example public endpoint pattern; replace with the source you choose
FX_API_URL = "https://api.frankfurter.dev/v1/latest?base=GBP"


def fetch_fx_payload(url: str) -> dict:
    req = Request(
        url,
        headers={
            "User-Agent": "financial-signals-lakehouse/1.0",
            "Accept": "application/json"
        },
    )

    with urlopen(req) as response:
        return json.loads(response.read().decode("utf-8"))


def normalise_payload(payload: dict, run_id: str) -> list[Row]:
    base = payload.get("base")
    rates = payload.get("rates", {})
    date_str = payload.get("date")

    rate_ts = None
    if date_str:
        # assumes YYYY-MM-DD
        rate_ts = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)

    ingested_at = datetime.now(timezone.utc)

    rows: list[Row] = []
    for quote_currency, rate in rates.items():
        rows.append(
            Row(
                base_currency=str(base),
                quote_currency=str(quote_currency),
                rate=float(rate) if rate is not None else None,
                rate_timestamp=rate_ts,
                source_name="exchangerate.host",
                source_url=FX_API_URL,
                ingested_at=ingested_at,
                run_id=run_id,
            )
        )
    return rows


def ensure_table_exists(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
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

    payload = fetch_fx_payload(FX_API_URL)
    rows = normalise_payload(payload, run_id)

    

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

    print(f"Wrote {df.count()} rows to {BRONZE_FX_RAW} with run_id={run_id}")


if __name__ == "__main__":
    main()

