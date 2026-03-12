import requests
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime

from src.common.config import BRONZE_MARKET_RAW
from src.common.audit import log_pipeline_run


API_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


def fetch_market_data(symbols):
    """
    Fetch market data from Yahoo Finance public endpoint.
    """

    params = {"symbols": ",".join(symbols)}

    r = requests.get(API_URL, params=params)
    r.raise_for_status()

    results = r.json()["quoteResponse"]["result"]

    records = []

    for r in results:
        records.append({
            "symbol": r.get("symbol"),
            "price": r.get("regularMarketPrice"),
            "currency": r.get("currency"),
            "market_time": datetime.utcfromtimestamp(
                r.get("regularMarketTime")
            ),
            "ingest_ts": datetime.utcnow()
        })

    return pd.DataFrame(records)


def main():

    spark = SparkSession.builder.getOrCreate()

    symbols = ["SPY", "QQQ", "IWM", "DIA"]

    pdf = fetch_market_data(symbols)

    row_count = len(pdf)

    df = spark.createDataFrame(pdf)

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