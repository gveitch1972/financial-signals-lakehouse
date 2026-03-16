CREATE TABLE IF NOT EXISTS fin_signals_dev.gold.macro_indicator_trends (
    country_code STRING,
    indicator_name STRING,
    observation_date DATE,
    observation_value DOUBLE,
    prev_value DOUBLE,
    change_value DOUBLE,
    change_pct DOUBLE,
    trend_direction STRING,
    raw_payload STRING,
    ingest_ts TIMESTAMP,
    _ingest_date DATE,
    _source STRING
)
USING DELTA;