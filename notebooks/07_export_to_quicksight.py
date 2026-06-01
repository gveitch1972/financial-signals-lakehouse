# Databricks notebook source
# Export Gold tables to S3 for QuickSight (Athena/Parquet) and React dashboard (JSON)
#
# Prerequisites:
#   - IAM user: databricks-s3-writer (eu-west-2)
#   - S3 bucket: fin-signals-quicksight-313753089884
#   - Athena results bucket: fin-signals-athena-results-313753089884
#   - Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in Databricks Secrets or env before running

# COMMAND ----------

import pandas as pd
import boto3
import os
import json
from datetime import date, timedelta

AWS_ACCESS_KEY = dbutils.secrets.get("aws", "s3-writer-key-id")
AWS_SECRET_KEY = dbutils.secrets.get("aws", "s3-writer-secret")
BUCKET = "fin-signals-quicksight-313753089884"
REGION = "eu-west-2"
JSON_LOOKBACK_DAYS = 365  # cap dashboard JSON to last N days

def get_s3():
    return boto3.client('s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION)

def export_table(table_name: str, s3_key: str):
    tmp_path = f"/tmp/{table_name}.parquet"
    rows = spark.sql(f"SELECT * FROM fin_signals_dev.gold.{table_name}").collect()
    pdf = pd.DataFrame([r.asDict() for r in rows])
    pdf.to_parquet(tmp_path, index=False)
    get_s3().upload_file(tmp_path, BUCKET, s3_key)
    print(f"Uploaded {table_name} → s3://{BUCKET}/{s3_key}  ({len(pdf)} rows)")
    return pdf

def trim_to_lookback(pdf: pd.DataFrame, date_col: str) -> pd.DataFrame:
    cutoff = (date.today() - timedelta(days=JSON_LOOKBACK_DAYS)).isoformat()
    col = pdf[date_col].astype(str)
    return pdf[col >= cutoff].reset_index(drop=True)

def export_json(pdf: pd.DataFrame, s3_key: str):
    pdf_json = pdf.copy()
    for col in pdf_json.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]", "object"]).columns:
        pdf_json[col] = pdf_json[col].astype(str)
    for col in pdf_json.select_dtypes(include=["datetime64[ns, UTC]"]).columns:
        pdf_json[col] = pdf_json[col].astype(str)
    for col in pdf_json.columns:
        if hasattr(pdf_json[col], 'dt'):
            pdf_json[col] = pdf_json[col].astype(str)

    records = [
        {k: (None if isinstance(v, float) and v != v else v) for k, v in row.items()}
        for row in pdf_json.to_dict(orient="records")
    ]
    body = json.dumps(records, default=str)
    get_s3().put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
        CacheControl="max-age=300"
    )
    print(f"Uploaded JSON → s3://{BUCKET}/{s3_key}  ({len(records)} records)")

# COMMAND ----------

pdf_snapshot = export_table("daily_market_snapshot", "daily_market_snapshot/data.parquet")
export_json(trim_to_lookback(pdf_snapshot, "snapshot_date"), "dashboard/daily_market_snapshot.json")

# COMMAND ----------

pdf_fx = export_table("fx_trend_signals", "fx_trend_signals/data.parquet")
export_json(trim_to_lookback(pdf_fx, "rate_date"), "dashboard/fx_trend_signals.json")

# COMMAND ----------

pdf_cross = export_table("cross_signal_summary", "cross_signal_summary/data.parquet")
export_json(trim_to_lookback(pdf_cross, "as_of_date"), "dashboard/cross_signal_summary.json")

# COMMAND ----------

export_table("macro_indicator_trends", "macro_indicator_trends/data.parquet")

# COMMAND ----------

pdf_movers = export_table("top_movers_why", "top_movers_why/data.parquet")
export_json(trim_to_lookback(pdf_movers, "as_of_date"), "dashboard/top_movers_why.json")
