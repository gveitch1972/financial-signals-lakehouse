# Databricks notebook source
# Export Gold tables to S3 for QuickSight via Athena
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

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_ACCESS_KEY")
BUCKET = "fin-signals-quicksight-313753089884"
REGION = "eu-west-2"

def export_table(table_name: str, s3_key: str):
    tmp_path = f"/tmp/{table_name}.parquet"
    rows = spark.sql(f"SELECT * FROM fin_signals_dev.gold.{table_name}").collect()
    pdf = pd.DataFrame([r.asDict() for r in rows])
    pdf.to_parquet(tmp_path)

    s3 = boto3.client('s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION)
    s3.upload_file(tmp_path, BUCKET, s3_key)
    print(f"Uploaded {table_name} → s3://{BUCKET}/{s3_key}  ({len(pdf)} rows)")

# COMMAND ----------

export_table("daily_market_snapshot", "daily_market_snapshot/data.parquet")

# COMMAND ----------

# Uncomment to export additional tables:
# export_table("fx_trend_signals",      "fx_trend_signals/data.parquet")
# export_table("cross_signal_summary",  "cross_signal_summary/data.parquet")
# export_table("macro_indicator_trends","macro_indicator_trends/data.parquet")
# export_table("top_movers_why",        "top_movers_why/data.parquet")
