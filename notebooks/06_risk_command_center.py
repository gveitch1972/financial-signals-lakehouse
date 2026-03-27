# Databricks notebook source

# Financial Signals Lakehouse: Risk Command Center
#
# Focus:
# 1) headline risk KPIs
# 2) trend snapshots
# 3) top movers with context
# 4) pipeline audit health

import os
import sys

from pyspark.sql import SparkSession

try:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:
    repo_root = "/Workspace/Users/microsoft@grahamveitch.com/.bundle/financial-signals-lakehouse/dev/files"

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.common.config import (
    AUDIT_PIPELINE_RUNS,
    GOLD_CROSS_SIGNALS,
    GOLD_FX_TRENDS,
    GOLD_TOP_MOVERS_WHY,
)


spark = SparkSession.builder.getOrCreate()


def heading(title):
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def render(df, limit=20):
    try:
        display(df.limit(limit))
    except NameError:
        df.show(limit, truncate=False)


def sql_df(query):
    return spark.sql(query)


heading("1. Headline KPIs")
kpis = sql_df(
    f"""
    WITH latest_date AS (
      SELECT MAX(as_of_date) AS as_of_date
      FROM {GOLD_CROSS_SIGNALS}
    ),
    cross_latest AS (
      SELECT c.*
      FROM {GOLD_CROSS_SIGNALS} c
      INNER JOIN latest_date d ON c.as_of_date = d.as_of_date
    ),
    fx_latest AS (
      SELECT f.*
      FROM {GOLD_FX_TRENDS} f
      INNER JOIN latest_date d ON f.rate_date = d.as_of_date
    )
    SELECT
      c.as_of_date,
      c.risk_regime,
      c.market_stress_symbols,
      c.fx_stress_pairs,
      c.macro_up_indicators,
      c.macro_down_indicators
    FROM cross_latest c
    """
)
render(kpis, limit=10)


heading("2. Trend Snapshot (Recent Dates)")
trends = sql_df(
    f"""
    SELECT
      as_of_date,
      market_avg_return_30d_pct,
      fx_stress_pairs,
      macro_up_indicators,
      macro_down_indicators,
      risk_regime
    FROM {GOLD_CROSS_SIGNALS}
    ORDER BY as_of_date DESC
    LIMIT 30
    """
)
render(trends, limit=30)


heading("3. Top Movers + Why")
top_movers = sql_df(
    f"""
    SELECT
      as_of_date,
      symbol,
      latest_price,
      day_change_pct,
      return_30d_pct,
      stress_flag,
      fx_context,
      macro_context,
      why_summary
    FROM {GOLD_TOP_MOVERS_WHY}
    ORDER BY as_of_date DESC, return_30d_pct ASC, symbol ASC
    LIMIT 20
    """
)
render(top_movers, limit=20)


heading("4. Pipeline Health (Audit)")
pipeline_latest = sql_df(
    f"""
    WITH ranked AS (
      SELECT
        pipeline_name,
        status,
        row_count,
        message,
        run_timestamp,
        ROW_NUMBER() OVER (PARTITION BY pipeline_name ORDER BY run_timestamp DESC) AS rn
      FROM {AUDIT_PIPELINE_RUNS}
    )
    SELECT
      pipeline_name,
      status,
      row_count,
      run_timestamp,
      message
    FROM ranked
    WHERE rn = 1
    ORDER BY run_timestamp DESC
    """
)
render(pipeline_latest, limit=50)

recent_failures = sql_df(
    f"""
    SELECT
      pipeline_name,
      status,
      row_count,
      run_timestamp,
      message
    FROM {AUDIT_PIPELINE_RUNS}
    WHERE status = 'FAILED'
    ORDER BY run_timestamp DESC
    LIMIT 20
    """
)
render(recent_failures, limit=20)
