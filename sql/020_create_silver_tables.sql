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