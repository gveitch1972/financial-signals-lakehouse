CREATE TABLE IF NOT EXISTS fin_signals_dev.silver.market_prices (
    symbol STRING,
    price DECIMAL(12,4),
    currency STRING,
    market_time TIMESTAMP,
    ingested_at TIMESTAMP,
    _ingest_date DATE,
    _source STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS fin_signals_dev.silver.fx_rates (
    currency_pair STRING,
    base_currency STRING,
    quote_currency STRING,
    rate DECIMAL(12,5),
    rate_timestamp TIMESTAMP,
    rate_date DATE,
    source_system STRING,
    ingested_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS fin_signals_dev.silver.macro_indicators (
    country_code STRING,
    indicator_name STRING,
    observation_date DATE,
    observation_value DOUBLE,
    raw_payload STRING,
    ingest_ts TIMESTAMP,
    _ingest_date DATE,
    _source STRING
)
USING DELTA;
