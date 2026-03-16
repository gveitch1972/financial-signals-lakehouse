import json
import os
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.common.config import BRONZE_MACRO_RAW

WORLD_BANK_SOURCE_URI = "https://api.worldbank.org/v2/country/all/indicator"
WORLD_BANK_API_URL = os.getenv("MACRO_API_URL", WORLD_BANK_SOURCE_URI)
WORLD_BANK_SOURCE_ID = os.getenv("MACRO_SOURCE_ID", "2")
DEFAULT_INDICATOR_CODES = [
    "SP.POP.TOTL",
    "NY.GDP.MKTP.CD",
    "FP.CPI.TOTL.ZG",
]
DEFAULT_LOOKBACK_YEARS = int(os.getenv("MACRO_LOOKBACK_YEARS", "10"))
DEFAULT_PAGE_SIZE = int(os.getenv("MACRO_PAGE_SIZE", "1000"))
HTTP_HEADERS = {
    "User-Agent": "financial-signals-lakehouse/1.0",
    "Accept": "application/json",
}

def _pick(record, *keys):
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return None


def parse_indicator_codes():
    raw_codes = os.getenv("MACRO_INDICATOR_CODES")
    if not raw_codes:
        return DEFAULT_INDICATOR_CODES

    codes = []
    for chunk in raw_codes.replace(";", ",").split(","):
        code = chunk.strip()
        if code:
            codes.append(code)
    return codes or DEFAULT_INDICATOR_CODES


def default_date_range(lookback_years=DEFAULT_LOOKBACK_YEARS):
    end_year = datetime.now(timezone.utc).year - 1
    start_year = end_year - max(lookback_years - 1, 0)
    return f"{start_year}:{end_year}"


def build_world_bank_url(page=1, per_page=DEFAULT_PAGE_SIZE, date_range=None):
    indicator_codes = ";".join(parse_indicator_codes())
    params = {
        "format": "json",
        "page": page,
        "per_page": per_page,
        "source": WORLD_BANK_SOURCE_ID,
        "date": date_range or default_date_range(),
    }
    return f"{WORLD_BANK_API_URL}/{indicator_codes}?{urlencode(params)}"


def fetch_json(url):
    request = Request(url, headers=HTTP_HEADERS)
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def normalize_world_bank_payload(payload):
    if not isinstance(payload, list) or len(payload) < 2:
        raise RuntimeError("World Bank API returned an unexpected payload shape.")

    metadata = payload[0] or {}
    rows = payload[1] or []
    page_count = int(metadata.get("pages", 1))

    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        observation_value = _pick(row, "value")
        normalized.append(
            {
                "country_code": _pick(row, "countryiso3code") or _pick(row.get("country", {}), "id"),
                "indicator_name": _pick(row.get("indicator", {}), "value", "id"),
                "observation_date": _pick(row, "date"),
                "observation_value": float(observation_value) if observation_value is not None else None,
                "raw_payload": json.dumps(row),
            }
        )

    return normalized, page_count


def fetch_macro_records():
    page = 1
    page_count = 1
    records = []

    while page <= page_count:
        url = build_world_bank_url(page=page)
        payload = fetch_json(url)
        page_records, page_count = normalize_world_bank_payload(payload)
        records.extend(page_records)
        page += 1

    return records


def safe_log_pipeline_run(spark, pipeline_name, status, row_count, message=None):
    from src.common.audit import log_pipeline_run

    try:
        log_pipeline_run(
            spark,
            pipeline_name=pipeline_name,
            status=status,
            row_count=row_count,
            message=message,
        )
    except Exception as log_error:
        print(f"Audit logging skipped due to error: {log_error}")


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp, lit, to_date
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    macro_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("observation_date", StringType(), True),
        StructField("observation_value", DoubleType(), True),
        StructField("raw_payload", StringType(), True),
    ])

    spark = SparkSession.builder.getOrCreate()

    try:
        records = fetch_macro_records()
        if not records:
            raise RuntimeError("Macro ingestion returned zero records from the World Bank API.")

        df = spark.createDataFrame(records, schema=macro_schema)

        enriched = (
            df.withColumn("ingest_ts", current_timestamp())
            .withColumn("_ingest_date", to_date(current_timestamp()))
            .withColumn("_source", lit(WORLD_BANK_SOURCE_URI))
        )

        (
            enriched.write.format("delta")
            .mode("append")
            .saveAsTable(BRONZE_MACRO_RAW)
        )

        row_count = enriched.count()
        safe_log_pipeline_run(
            spark,
            pipeline_name="bronze_macro_ingest",
            status="SUCCESS",
            row_count=row_count,
        )
    except HTTPError as error:
        message = f"HTTP {error.code} calling macro source: {error.reason}"
        safe_log_pipeline_run(
            spark,
            pipeline_name="bronze_macro_ingest",
            status="FAILED",
            row_count=0,
            message=message,
        )
        raise RuntimeError(message) from error
    except Exception as error:
        safe_log_pipeline_run(
            spark,
            pipeline_name="bronze_macro_ingest",
            status="FAILED",
            row_count=0,
            message=str(error),
        )
        raise


if __name__ == "__main__":
    main()
