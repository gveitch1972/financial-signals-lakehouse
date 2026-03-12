CREATE TABLE IF NOT EXISTS fin_signals_dev.bronze.market_prices_raw (
    symbol STRING,
    price DOUBLE,
    currency STRING,
    market_time TIMESTAMP,
    ingest_ts TIMESTAMP
)
USING DELTA;