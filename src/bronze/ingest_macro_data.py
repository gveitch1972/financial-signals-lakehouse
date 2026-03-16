import json
import os
from urllib.parse import urlencode
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date

from src.common.audit import log_pipeline_run
from src.common.config import BRONZE_MACRO_RAW

DATA360_SOURCE_URI = "https://data360.worldbank.org/en/api#/Data/get_data360_data"
DATA360_API_URL = os.getenv("MACRO_DATA360_API_URL", "https://data360api.worldbank.org/v1/data")


def _pick(record, *keys):
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def fetch_macro_records(limit=500):
    params = urlencode({"limit": limit})
    url = f"{DATA360_API_URL}?{params}"

    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if isinstance(payload, dict):
        records = payload.get("data", [])
    else:
        records = payload

    normalized = []
    for row in records:
        if not isinstance(row, dict):
            continue

        normalized.append(
            {
                "country_code": _pick(row, "country_code", "country", "iso3c"),
                "indicator_name": _pick(row, "indicator_name", "indicator", "series_name"),
                "observation_date": _pick(row, "observation_date", "date", "time_period"),
                "observation_value": _pick(row, "observation_value", "value"),
                "raw_payload": json.dumps(row),
            }
        )

    return normalized


def main():
    spark = SparkSession.builder.getOrCreate()

    records = fetch_macro_records()
    df = spark.createDataFrame(records)

    enriched = (
        df.withColumn("ingest_ts", current_timestamp())
        .withColumn("_ingest_date", to_date(current_timestamp()))
        .withColumn("_source", lit(DATA360_SOURCE_URI))
    )

    (
        enriched.write.format("delta")
        .mode("append")
        .saveAsTable(BRONZE_MACRO_RAW)
    )

    log_pipeline_run(
        spark,
        pipeline_name="bronze_macro_ingest",
        status="SUCCESS",
        row_count=enriched.count(),
    )


if __name__ == "__main__":
    main()