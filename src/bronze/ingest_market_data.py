import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

from src.common.config import BRONZE_MARKET_RAW
from src.common.audit import log_pipeline_run

import os 

print("MARKER: ingest-market-v3")
print("INGEST FILE:", __file__ if "__file__" in globals() else "no __file__")
print("INGEST CWD:", os.getcwd())

STOOQ_URL = "https://stooq.com/q/l/"

raw_schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
])

def fetch_market_data_spark(spark, symbols):
    params = {
        "s": ",".join(symbols).lower(),
        "f": "sd2t2ohlcv",
        "h": "",
        "e": "csv"
    }

    r = requests.get(STOOQ_URL, params=params, timeout=30)
    r.raise_for_status()

    lines = [x for x in r.text.splitlines() if x.strip()]
    rdd = spark.sparkContext.parallelize(lines)

    df = (
        spark.read
        .option("header", True)
        .schema(raw_schema)
        .csv(rdd)
    )

    return (
        df.withColumnRenamed("Symbol", "symbol")
          .withColumnRenamed("Close", "price")
          .withColumn("currency", lit("USD"))
          .withColumn(
              "market_time",
              to_timestamp(concat_ws(" ", col("Date"), col("Time")), "yyyy-MM-dd HH:mm:ss")
          )
          .withColumn("ingest_ts", current_timestamp())
          .select("symbol", "price", "currency", "market_time", "ingest_ts")
    )

def main():
    spark = SparkSession.builder.getOrCreate()
    symbols = ["SPY.US"]

    print("MARKER: entering main()")

    df = fetch_market_data_spark(spark, symbols)

    df.printSchema()
    df.show(truncate=False)

    row_count = df.count()

    (
        df.write
          .format("delta")
          .mode("append")
          .saveAsTable(BRONZE_MARKET_RAW)
    )

    log_pipeline_run(
        spark,
        pipeline_name="bronze_market_ingest",
        status="SUCCESS",
        row_count=row_count
    )

if __name__ == "__main__":
    main()
