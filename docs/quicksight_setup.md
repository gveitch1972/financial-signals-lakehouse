# QuickSight Setup — Financial Signals

Connects AWS QuickSight to Databricks Gold tables via S3 + Athena.

---

## Architecture

```
Databricks Gold (fin_signals_dev.gold)
  → boto3 export → S3 (fin-signals-quicksight-313753089884)
  → Athena external table
  → QuickSight SPICE dataset
  → Dashboard
```

---

## One-time Infrastructure

### S3 buckets (eu-west-2)
```bash
aws s3 mb s3://fin-signals-quicksight-313753089884 --region eu-west-2
aws s3 mb s3://fin-signals-athena-results-313753089884 --region eu-west-2
```

### IAM user for Databricks → S3 writes
```bash
aws iam create-user --user-name databricks-s3-writer

aws iam put-user-policy \
  --user-name databricks-s3-writer \
  --policy-name fin-signals-s3-write \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::fin-signals-quicksight-313753089884",
        "arn:aws:s3:::fin-signals-quicksight-313753089884/*"
      ]
    }]
  }'

aws iam create-access-key --user-name databricks-s3-writer
# Save AccessKeyId + SecretAccessKey — shown once only
```

### QuickSight service role — S3 access
QuickSight's managed role needs an explicit inline policy (the UI bucket-grant alone is insufficient):
```bash
aws iam put-role-policy \
  --role-name aws-quicksight-service-role-v0 \
  --policy-name fin-signals-s3-access \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::fin-signals-quicksight-313753089884",
        "arn:aws:s3:::fin-signals-quicksight-313753089884/*"
      ]
    }]
  }'
```

Also grant bucket access in QuickSight UI: Manage QuickSight → Security & permissions → AWS resources → S3 → select bucket.

---

## Exporting Gold Tables

Run `notebooks/07_export_to_quicksight.py` in Databricks. Set env vars before running:
```
AWS_ACCESS_KEY_ID=<databricks-s3-writer key>
AWS_SECRET_ACCESS_KEY=<databricks-s3-writer secret>
```

### Gotchas on Databricks Serverless

| Approach | Result |
|----------|--------|
| `spark.conf.set("fs.s3a.access.key", ...)` | Blocked — `Configuration not available` error |
| `sc._jsc.hadoopConfiguration().set(...)` | Blocked — `sc not supported on serverless` |
| `df.toPandas()` | Fails — `PlanMetrics not JSON serializable` |
| `spark.sql(...).toPandas()` | Also fails — same error |
| `.collect()` + `pd.DataFrame([r.asDict() for r in rows])` | **Works** — bypasses Arrow serialization |

---

## Athena Setup

Set results location (first time only):
```
s3://fin-signals-athena-results-313753089884/
```

Create external table:
```sql
CREATE EXTERNAL TABLE daily_market_snapshot (
  symbol STRING,
  snapshot_date DATE,
  latest_price DOUBLE,
  open_price DOUBLE,
  day_change DOUBLE,
  day_change_pct DOUBLE,
  return_7d_pct DOUBLE,
  return_30d_pct DOUBLE,
  return_90d_pct DOUBLE,
  rolling_30d_volatility DOUBLE,
  drawdown_from_90d_high_pct DOUBLE,
  stress_flag BOOLEAN,
  currency STRING,
  market_time TIMESTAMP,
  ingested_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://fin-signals-quicksight-313753089884/daily_market_snapshot/';
```

---

## Athena Tables — Additional Gold Tables

Run these in Athena after exporting from Databricks:

```sql
CREATE EXTERNAL TABLE fx_trend_signals (
  currency_pair          STRING,
  rate_date              DATE,
  base_currency          STRING,
  quote_currency         STRING,
  rate                   DOUBLE,
  daily_change           DOUBLE,
  daily_change_pct       DOUBLE,
  weekly_change_pct      DOUBLE,
  return_30d_pct         DOUBLE,
  rolling_30d_volatility DOUBLE,
  trend_signal           STRING,
  stress_flag            BOOLEAN,
  ingested_at            TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://fin-signals-quicksight-313753089884/fx_trend_signals/';

CREATE EXTERNAL TABLE cross_signal_summary (
  signal_date            DATE,
  stressed_equity_count  INT,
  stressed_fx_count      INT,
  equity_stress_pct      DOUBLE,
  fx_stress_pct          DOUBLE,
  risk_regime            STRING,
  ingested_at            TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://fin-signals-quicksight-313753089884/cross_signal_summary/';

CREATE EXTERNAL TABLE macro_indicator_trends (
  country_code           STRING,
  indicator_name         STRING,
  observation_date       DATE,
  observation_value      DOUBLE,
  period_change          DOUBLE,
  period_change_pct      DOUBLE,
  year_over_year_pct     DOUBLE,
  trend_direction        STRING,
  ingested_at            TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://fin-signals-quicksight-313753089884/macro_indicator_trends/';

CREATE EXTERNAL TABLE top_movers_why (
  as_of_date             DATE,
  symbol                 STRING,
  latest_price           DOUBLE,
  day_change_pct         DOUBLE,
  return_30d_pct         DOUBLE,
  stress_flag            BOOLEAN,
  fx_context             STRING,
  macro_context          STRING,
  why_summary            STRING
)
STORED AS PARQUET
LOCATION 's3://fin-signals-quicksight-313753089884/top_movers_why/';
```

---

## QuickSight Dataset

Datasets → New dataset → Athena → workgroup: `primary` → database: `default` → table: `daily_market_snapshot` → Import to SPICE.

**Note:** `daily_market_snapshot` is a current-state snapshot — one row per symbol per pipeline run. For time-series visuals, export `fx_trend_signals` instead.

---

## Dashboard

Published at: `eu-west-2.quicksight.aws.amazon.com/sn/accounts/313753089884/dashboards/...`

Visuals:
- Horizontal bar chart: `day_change_pct` by symbol (today's movers)
- Line chart: `latest_price` over time by symbol
- KPI strip: current price per symbol

**KPI tip:** Keep the KPI visual short (2-3 rows height) — tall KPI boxes show the trend sparkline which dominates the layout.
