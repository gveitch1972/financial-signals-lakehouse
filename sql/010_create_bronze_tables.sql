CREATE TABLE IF NOT EXISTS fin_signals_dev.bronze.market_prices_raw (
    symbol STRING,
    price DOUBLE,
    currency STRING,
    market_time TIMESTAMP,
    ingest_ts TIMESTAMP,
    _ingest_date TIMESTAMP,
    _source STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS fin_signals_dev.bronze.fx_rates_raw (
    base_currency STRING NOT NULL,
    quote_currency STRING NOT NULL,
    rate DOUBLE,
    rate_timestamp TIMESTAMP,
    source_name STRING,
    source_url STRING,
    ingested_at TIMESTAMP NOT NULL,
    run_id STRING NOT NULL
)
USING DELTA;