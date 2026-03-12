CREATE TABLE IF NOT EXISTS fin_signals_dev.audit.pipeline_runs (
    pipeline_name STRING,
    status STRING,
    row_count INT,
    message STRING,
    run_timestamp TIMESTAMP
)
USING DELTA;