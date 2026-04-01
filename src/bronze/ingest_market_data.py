import os
import re
from datetime import date

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.audit import log_pipeline_run
from src.common.config import BRONZE_MARKET_RAW

STOOQ_SNAPSHOT_URL = "https://stooq.com/q/l/"
STOOQ_HISTORY_URL = "https://stooq.com/q/d/l/"
DEFAULT_MARKET_SYMBOLS = [
    "SPY.US",
    "QQQ.US",
    "IWM.US",
    "GLD.US",
    "TLT.US",
    "EFA.US",
    "VIX.US",  # volatility
    "DXY.US",  # dollar
    "USO.US",  # oil
]
DEFAULT_START_DATE = "2020-01-01"
HTTP_HEADERS = {"User-Agent": "financial-signals-lakehouse/1.0"}

snapshot_schema = StructType(
    [
        StructField("Symbol", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
    ]
)

history_schema = StructType(
    [
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
    ]
)


def parse_csv_env(name, default_values):
    raw_value = os.getenv(name)
    if not raw_value:
        return list(default_values)

    parsed = []
    for chunk in re.split(r"[\s,;]+", raw_value.strip()):
        value = chunk.strip().upper()
        if value:
            parsed.append(value)
    return parsed or list(default_values)


def get_load_mode():
    load_mode = os.getenv("LOAD_MODE", "snapshot").strip().lower()
    if load_mode not in {"snapshot", "backfill"}:
        raise ValueError(
            f"Unsupported LOAD_MODE '{load_mode}'. Expected 'snapshot' or 'backfill'."
        )
    return load_mode


def get_date_range():
    start_date = os.getenv("START_DATE", DEFAULT_START_DATE)
    end_date = os.getenv("END_DATE", date.today().isoformat())
    if start_date > end_date:
        raise ValueError(
            f"START_DATE {start_date} must be on or before END_DATE {end_date}."
        )
    return start_date, end_date


def fetch_text(url, params):
    response = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


def read_csv_text(spark, csv_text, schema):
    lines = [line for line in csv_text.splitlines() if line.strip()]
    if not lines:
        return spark.createDataFrame([], schema)

    rdd = spark.sparkContext.parallelize(lines)
    return spark.read.option("header", True).schema(schema).csv(rdd)


def standardize_market_frame(df, source_name):
    standardized = (
        df.withColumnRenamed("Symbol", "symbol")
        .withColumnRenamed("Close", "price")
        .withColumn("currency", F.split(F.col("symbol"), r"\.").getItem(1))
        .withColumn(
            "market_time",
            F.coalesce(
                F.try_to_timestamp(
                    F.concat_ws(" ", F.col("Date"), F.col("Time")),
                    F.lit("yyyy-MM-dd HH:mm:ss"),
                ),
                F.try_to_timestamp(F.col("Date"), F.lit("yyyy-MM-dd")),
            ),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
        .withColumn("_source", F.lit(source_name))
        .select(
            "symbol",
            "price",
            "currency",
            "market_time",
            "ingested_at",
            "_ingest_date",
            "_source",
        )
    )

    return (
        standardized.filter(F.col("symbol").isNotNull())
        .filter(F.col("price").isNotNull())
        .filter(F.col("market_time").isNotNull())
    )


def empty_market_df(spark):
    return spark.createDataFrame(
        [],
        StructType(
            [
                StructField("symbol", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("market_time", StringType(), True),
                StructField("ingested_at", StringType(), True),
                StructField("_ingest_date", StringType(), True),
                StructField("_source", StringType(), True),
            ]
        ),
    ).select(
        F.col("symbol"),
        F.col("price"),
        F.col("currency"),
        F.col("market_time").cast("timestamp"),
        F.col("ingested_at").cast("timestamp"),
        F.col("_ingest_date").cast("date"),
        F.col("_source"),
    )


def fetch_snapshot_for_symbol(spark, symbol):
    params = {
        "s": symbol.lower(),
        "f": "sd2t2ohlcv",
        "h": "",
        "e": "csv",
    }
    csv_text = fetch_text(STOOQ_SNAPSHOT_URL, params)
    if "No data" in csv_text or len(csv_text.strip().splitlines()) <= 1:
        print(f"No history data for {symbol}")
        return empty_market_df(spark)

    df = read_csv_text(spark, csv_text, snapshot_schema)
    return standardize_market_frame(df, "stooq_snapshot")


def fetch_snapshot_market_data(spark, symbols):
    snapshot_frames = [fetch_snapshot_for_symbol(spark, symbol) for symbol in symbols]
    non_empty_frames = [frame for frame in snapshot_frames if frame.take(1)]
    if not non_empty_frames:
        return empty_market_df(spark)

    combined = non_empty_frames[0]
    for frame in non_empty_frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def fetch_history_for_symbol(spark, symbol, start_date, end_date):
    params = {"s": symbol.lower(), "i": "d"}
    csv_text = fetch_text(STOOQ_HISTORY_URL, params)
    history = read_csv_text(spark, csv_text, history_schema)

    print(f"Fetching history for {symbol}")
    print(f"Rows returned: {history.count()}")

    return (
        history.withColumn("symbol", F.lit(symbol.upper()))
        .withColumn("price", F.col("Close"))
        .withColumn("currency", F.split(F.col("symbol"), r"\.").getItem(1))
        .withColumn("market_date", F.to_date(F.col("Date"), "yyyy-MM-dd"))
        .filter(F.col("market_date").between(F.lit(start_date), F.lit(end_date)))
        .withColumn("market_time", F.to_timestamp(F.col("Date"), "yyyy-MM-dd"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
        .withColumn("_source", F.lit("stooq_backfill"))
        .select(
            "symbol",
            "price",
            "currency",
            "market_time",
            "ingested_at",
            "_ingest_date",
            "_source",
        )
    )


def fetch_backfill_market_data(spark, symbols, start_date, end_date):
    history_frames = [
        fetch_history_for_symbol(spark, symbol, start_date, end_date)
        for symbol in symbols
    ]

    non_empty = [df for df in history_frames if df.take(1)]

    if not non_empty:
        return empty_market_df(spark)

    combined = non_empty[0]
    for df in non_empty[1:]:
        combined = combined.unionByName(df)

    return combined


def safe_log_pipeline_run(spark, pipeline_name, status, row_count, message=None):
    try:
        log_pipeline_run(
            spark,
            pipeline_name=pipeline_name,
            status=status,
            row_count=row_count,
            message=message,
        )
    except Exception as error:
        print(f"Audit logging skipped due to error: {error}")


def main():
    spark = SparkSession.builder.getOrCreate()
    symbols = parse_csv_env("MARKET_SYMBOLS", DEFAULT_MARKET_SYMBOLS)
    load_mode = get_load_mode()
    start_date, end_date = get_date_range()

    try:
        if load_mode == "backfill":
            df = fetch_backfill_market_data(spark, symbols, start_date, end_date)
        else:
            df = fetch_snapshot_market_data(spark, symbols)

        if df.rdd.isEmpty():
            raise RuntimeError("Market ingestion returned zero rows.")

        (df.write.format("delta").mode("append").saveAsTable(BRONZE_MARKET_RAW))

        row_count = df.count()
        safe_log_pipeline_run(
            spark,
            pipeline_name=f"bronze_market_ingest_{load_mode}",
            status="SUCCESS",
            row_count=row_count,
            message=f"symbols={','.join(symbols)};start={start_date};end={end_date}",
        )
    except Exception as error:
        safe_log_pipeline_run(
            spark,
            pipeline_name=f"bronze_market_ingest_{load_mode}",
            status="FAILED",
            row_count=0,
            message=str(error),
        )
        raise


if __name__ == "__main__":
    main()
