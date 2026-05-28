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

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_ACCESS_KEY")
BUCKET = "fin-signals-quicksight-313753089884"
REGION = "eu-west-2"

def get_s3():
    return boto3.client('s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION)

def export_table(table_name: str, s3_key: str):
    tmp_path = f"/tmp/{table_name}.parquet"
    rows = spark.sql(f"SELECT * FROM fin_signals_dev.gold.{table_name}").collect()
    pdf = pd.DataFrame([r.asDict() for r in rows])
    pdf.to_parquet(tmp_path)
    get_s3().upload_file(tmp_path, BUCKET, s3_key)
    print(f"Uploaded {table_name} → s3://{BUCKET}/{s3_key}  ({len(pdf)} rows)")
    return pdf

def export_json(pdf: pd.DataFrame, s3_key: str):
    # Convert timestamps/dates to strings for JSON serialisation
    pdf_json = pdf.copy()
    for col in pdf_json.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]", "object"]).columns:
        pdf_json[col] = pdf_json[col].astype(str)
    for col in pdf_json.select_dtypes(include=["datetime64[ns, UTC]"]).columns:
        pdf_json[col] = pdf_json[col].astype(str)
    # Handle date columns
    for col in pdf_json.columns:
        if hasattr(pdf_json[col], 'dt'):
            pdf_json[col] = pdf_json[col].astype(str)

    # Replace NaN with None before serialising — NaN is not valid JSON
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
    print(f"Uploaded JSON → s3://{BUCKET}/{s3_key}")

# COMMAND ----------

pdf_snapshot = export_table("daily_market_snapshot", "daily_market_snapshot/data.parquet")
export_json(pdf_snapshot, "dashboard/daily_market_snapshot.json")

# COMMAND ----------

# Uncomment to export additional tables:
# pdf_fx = export_table("fx_trend_signals", "fx_trend_signals/data.parquet")
# export_json(pdf_fx, "dashboard/fx_trend_signals.json")

# export_table("cross_signal_summary",  "cross_signal_summary/data.parquet")
# export_table("macro_indicator_trends","macro_indicator_trends/data.parquet")
# export_table("top_movers_why",        "top_movers_why/data.parquet")
