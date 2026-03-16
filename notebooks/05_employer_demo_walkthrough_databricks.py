# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Signals Lakehouse: Employer Demo Walkthrough
# MAGIC
# MAGIC This notebook is formatted for a live Databricks demo with separate runnable cells.
# MAGIC
# MAGIC Suggested flow:
# MAGIC 1. Explain the business problem
# MAGIC 2. Show medallion architecture and run path
# MAGIC 3. Prove the pipeline is healthy
# MAGIC 4. Spend most of the time on Gold outputs
# MAGIC 5. Close on why this is a credible Databricks consulting delivery

# COMMAND ----------

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
    BRONZE_FX_RAW,
    BRONZE_MACRO_RAW,
    BRONZE_MARKET_RAW,
    GOLD_CROSS_SIGNALS,
    GOLD_FX_TRENDS,
    GOLD_MACRO_TRENDS,
    GOLD_MARKET_SNAPSHOT,
    SILVER_FX,
    SILVER_MACRO,
    SILVER_MARKET,
)

spark = SparkSession.builder.getOrCreate()


def show_df(df, limit=20):
    try:
        display(df.limit(limit))
    except NameError:
        df.show(limit, truncate=False)


def sql_df(query):
    return spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Project Goal And Consultancy Use Case
# MAGIC
# MAGIC **Problem statement**
# MAGIC
# MAGIC Treasury, risk, and advisory teams often have fragmented views of:
# MAGIC - market stress
# MAGIC - FX moves
# MAGIC - macro direction
# MAGIC
# MAGIC **What this project does**
# MAGIC
# MAGIC It unifies those domains into one Databricks platform:
# MAGIC - Bronze ingests public market, FX, and macro data
# MAGIC - Silver standardizes and deduplicates it
# MAGIC - Gold turns it into risk tables that support consulting-style conversations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Architecture And Orchestration Snapshot
# MAGIC
# MAGIC **Medallion flow**
# MAGIC
# MAGIC `Public APIs -> Bronze -> Silver -> Gold -> Validation`
# MAGIC
# MAGIC **Operational flow**
# MAGIC
# MAGIC 1. Bootstrap environment
# MAGIC 2. Historical backfill for price and FX
# MAGIC 3. Daily pipeline refresh
# MAGIC 4. Gold analytics and validation
# MAGIC
# MAGIC **Why Databricks**
# MAGIC
# MAGIC - scalable Spark transformations
# MAGIC - Delta Lake tables
# MAGIC - job orchestration
# MAGIC - repeatable bundle-based deployment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pipeline Proof: Row Counts By Layer

# COMMAND ----------

layer_counts = sql_df(
    f"""
    SELECT 'bronze_market' AS layer_name, COUNT(*) AS row_count FROM {BRONZE_MARKET_RAW}
    UNION ALL
    SELECT 'silver_market', COUNT(*) FROM {SILVER_MARKET}
    UNION ALL
    SELECT 'gold_market_snapshot', COUNT(*) FROM {GOLD_MARKET_SNAPSHOT}
    UNION ALL
    SELECT 'bronze_fx', COUNT(*) FROM {BRONZE_FX_RAW}
    UNION ALL
    SELECT 'silver_fx', COUNT(*) FROM {SILVER_FX}
    UNION ALL
    SELECT 'gold_fx_trends', COUNT(*) FROM {GOLD_FX_TRENDS}
    UNION ALL
    SELECT 'bronze_macro', COUNT(*) FROM {BRONZE_MACRO_RAW}
    UNION ALL
    SELECT 'silver_macro', COUNT(*) FROM {SILVER_MACRO}
    UNION ALL
    SELECT 'gold_macro_trends', COUNT(*) FROM {GOLD_MACRO_TRENDS}
    UNION ALL
    SELECT 'gold_cross_signal_summary', COUNT(*) FROM {GOLD_CROSS_SIGNALS}
    """
)

show_df(layer_counts, limit=20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Freshness Snapshot
# MAGIC
# MAGIC This is a quick proof that the pipeline has loaded recent market and FX data, and that macro is being handled as a lower-frequency series.

# COMMAND ----------

freshness = sql_df(
    f"""
    SELECT 'market' AS dataset, MAX(to_date(market_time)) AS latest_date FROM {SILVER_MARKET}
    UNION ALL
    SELECT 'fx', MAX(rate_date) FROM {SILVER_FX}
    UNION ALL
    SELECT 'macro', MAX(observation_date) FROM {SILVER_MACRO}
    UNION ALL
    SELECT 'cross_signal_summary', MAX(as_of_date) FROM {GOLD_CROSS_SIGNALS}
    """
)

show_df(freshness, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historical Backfill And Validation Context
# MAGIC
# MAGIC The employer story here is important:
# MAGIC
# MAGIC - this is not just latest snapshots
# MAGIC - price and FX have historical depth
# MAGIC - validation checks prove the outputs are trustworthy enough to demo repeatedly

# COMMAND ----------

validation_context = sql_df(
    f"""
    SELECT
      (SELECT COUNT(DISTINCT to_date(market_time)) FROM {SILVER_MARKET}) AS market_history_days,
      (SELECT COUNT(DISTINCT rate_date) FROM {SILVER_FX}) AS fx_history_days,
      (SELECT COUNT(DISTINCT observation_date) FROM {SILVER_MACRO}) AS macro_history_periods
    """
)

show_df(validation_context, limit=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Latest Cross-Domain Risk Regime
# MAGIC
# MAGIC This is the executive summary table.
# MAGIC
# MAGIC **Question answered**
# MAGIC
# MAGIC What is the current cross-domain risk regime, and how broad is the stress across market, FX, and macro?

# COMMAND ----------

latest_risk_regime = sql_df(
    f"""
    SELECT *
    FROM {GOLD_CROSS_SIGNALS}
    ORDER BY as_of_date DESC
    LIMIT 10
    """
)

show_df(latest_risk_regime, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Top Stressed Market Instruments
# MAGIC
# MAGIC **Question answered**
# MAGIC
# MAGIC Which instruments are under the most pressure based on recent returns, volatility, and drawdown?

# COMMAND ----------

top_stressed_market = sql_df(
    f"""
    SELECT
      symbol,
      snapshot_date,
      latest_price,
      return_30d_pct,
      return_90d_pct,
      rolling_30d_volatility,
      drawdown_from_90d_high_pct,
      stress_flag
    FROM {GOLD_MARKET_SNAPSHOT}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {GOLD_MARKET_SNAPSHOT})
    ORDER BY stress_flag DESC, drawdown_from_90d_high_pct ASC, return_30d_pct ASC
    LIMIT 10
    """
)

show_df(top_stressed_market, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Top FX Signals
# MAGIC
# MAGIC **Question answered**
# MAGIC
# MAGIC Which currency pairs are moving sharply and where might treasury or hedging teams need to pay attention?

# COMMAND ----------

top_fx_signals = sql_df(
    f"""
    SELECT
      currency_pair,
      rate_date,
      rate,
      daily_change_pct,
      weekly_change_pct,
      return_30d_pct,
      rolling_30d_volatility,
      trend_signal,
      stress_flag
    FROM {GOLD_FX_TRENDS}
    WHERE rate_date = (SELECT MAX(rate_date) FROM {GOLD_FX_TRENDS})
    ORDER BY stress_flag DESC, ABS(return_30d_pct) DESC, ABS(daily_change_pct) DESC
    LIMIT 10
    """
)

show_df(top_fx_signals, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Recent Macro Trend View
# MAGIC
# MAGIC **Question answered**
# MAGIC
# MAGIC Which macro indicators are moving, and how much context do they add to the current risk regime?

# COMMAND ----------

macro_trends = sql_df(
    f"""
    SELECT
      country_code,
      indicator_name,
      observation_date,
      observation_value,
      period_change_pct,
      year_over_year_pct,
      trend_direction
    FROM {GOLD_MACRO_TRENDS}
    ORDER BY observation_date DESC, country_code, indicator_name
    LIMIT 15
    """
)

show_df(macro_trends, limit=15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Recent 30d And 90d Movers
# MAGIC
# MAGIC This gives a more consulting-friendly “what changed materially?” view for recent market performance.

# COMMAND ----------

recent_movers = sql_df(
    f"""
    SELECT
      symbol,
      snapshot_date,
      return_30d_pct,
      return_90d_pct,
      rolling_30d_volatility
    FROM {GOLD_MARKET_SNAPSHOT}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {GOLD_MARKET_SNAPSHOT})
    ORDER BY return_30d_pct ASC, return_90d_pct ASC
    LIMIT 10
    """
)

show_df(recent_movers, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Consultancy Talking Points
# MAGIC
# MAGIC **`daily_market_snapshot`**
# MAGIC - Answers: which instruments are stressed, how deep the drawdown is, and how volatile the recent move has been
# MAGIC - Audience: risk, asset management, investment committees
# MAGIC - Action: review stressed names, hedges, and concentration
# MAGIC
# MAGIC **`fx_trend_signals`**
# MAGIC - Answers: which pairs are strengthening or weakening and whether volatility is building
# MAGIC - Audience: treasury, finance, hedging teams
# MAGIC - Action: revisit hedge timing, funding assumptions, and currency commentary
# MAGIC
# MAGIC **`macro_indicator_trends`**
# MAGIC - Answers: whether macro indicators are improving or deteriorating and how strong the shift is
# MAGIC - Audience: strategy, economics, advisory teams
# MAGIC - Action: frame the market and FX story with macro context
# MAGIC
# MAGIC **`cross_signal_summary`**
# MAGIC - Answers: what the current risk regime is and how broad the stress is across domains
# MAGIC - Audience: board reporting, portfolio oversight, advisory leadership
# MAGIC - Action: use as a top-of-pack summary before deeper drill-down

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Example Business Takeaways
# MAGIC
# MAGIC 1. The platform turns three public domains into one advisory-ready risk regime summary.
# MAGIC 2. Market and FX moves can be discussed together, which is closer to how real treasury and risk conversations happen.
# MAGIC 3. Historical backfill means the Gold layer supports trend, volatility, and drawdown analysis rather than single-day snapshots.
# MAGIC 4. Validation makes the delivery feel operationally credible, not just visually impressive.
# MAGIC 5. The design is small enough to explain quickly but already structured like a client delivery foundation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Why This Is A Credible Databricks Delivery
# MAGIC
# MAGIC **Engineering credibility already proven**
# MAGIC - Databricks Asset Bundles deployment
# MAGIC - medallion architecture
# MAGIC - historical backfill plus daily refresh
# MAGIC - deterministic Silver transformations
# MAGIC - Gold analytical tables with clear business semantics
# MAGIC - automated validation checks
# MAGIC
# MAGIC **What a consultancy could do next**
# MAGIC - add SQL dashboard and board pack views
# MAGIC - extend source coverage with rates or credit signals
# MAGIC - add AI commentary on weekly regime shifts
# MAGIC - productionize with CI, promotion, and alerting

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Suggested Live Demo Finish
# MAGIC
# MAGIC End with:
# MAGIC
# MAGIC - why these three domains were chosen
# MAGIC - why Databricks was the right platform
# MAGIC - what engineering maturity is already present
# MAGIC - what a client-ready next phase would look like
