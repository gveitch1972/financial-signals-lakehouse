# Databricks notebook source

# Financial Signals Lakehouse: Employer Demo Walkthrough
#
# This notebook is designed for a live 10-15 minute walkthrough with a
# Databricks consulting-style audience. It keeps the platform story concise
# and spends most of the time on the Gold outputs and what they mean.

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


def heading(title):
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def subheading(title):
    print("\n" + title)
    print("-" * len(title))


def render(df, limit=20):
    try:
        display(df.limit(limit))
    except NameError:
        df.show(limit, truncate=False)


def sql_df(query):
    return spark.sql(query)


heading("1. Project Goal And Consultancy Use Case")
print(
    """
This project demonstrates a market + FX + macro risk intelligence platform on Databricks.

The consultancy story:
- Treasury teams need timely FX and macro context around exposures and funding decisions.
- Risk teams need market stress and volatility signals with historical depth.
- Advisory teams need a concise cross-domain view they can use in client updates and board reporting.

The platform answer:
- Bronze ingests public market, FX, and macro sources.
- Silver standardizes and deduplicates the data.
- Gold publishes risk tables that are easier to interpret and discuss with clients.
""".strip()
)


heading("2. Architecture And Orchestration Snapshot")
print(
    """
Medallion flow:
Public APIs -> Bronze -> Silver -> Gold -> Validation

Operational flow:
1. Bootstrap environment
2. Historical backfill for price and FX
3. Daily pipeline refresh
4. Gold analytics and validation

Why Databricks fits:
- managed orchestration
- Delta Lake tables across medallion layers
- scalable Spark transformations
- simple path from raw ingestion to analytical outputs
""".strip()
)


heading("3. Bronze, Silver, And Gold Proof")
subheading("Layer Row Counts")
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
render(layer_counts, limit=20)

subheading("Freshness Snapshot")
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
render(freshness, limit=10)


heading("4. Historical Backfill And Validation Proof")
print(
    """
This demo includes a one-time historical backfill job for price and FX so Gold analytics are based on
meaningful history rather than latest snapshots only.

Validation now checks:
- non-zero row counts
- freshness
- duplicate business keys
- history depth
- Gold metric completeness
""".strip()
)

validation_context = sql_df(
    f"""
    SELECT
      (SELECT COUNT(DISTINCT to_date(market_time)) FROM {SILVER_MARKET}) AS market_history_days,
      (SELECT COUNT(DISTINCT rate_date) FROM {SILVER_FX}) AS fx_history_days,
      (SELECT COUNT(DISTINCT observation_date) FROM {SILVER_MACRO}) AS macro_history_periods
    """
)
render(validation_context, limit=5)


heading("5. Gold Risk Outputs")
subheading("Latest Risk Regime")
latest_risk_regime = sql_df(
    f"""
    SELECT *
    FROM {GOLD_CROSS_SIGNALS}
    ORDER BY as_of_date DESC
    LIMIT 10
    """
)
render(latest_risk_regime, limit=10)

subheading("Top Stressed Market Instruments")
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
render(top_stressed_market, limit=10)

subheading("Top FX Signals")
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
render(top_fx_signals, limit=10)

subheading("Recent Macro Trend View")
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
render(macro_trends, limit=15)

subheading("Recent 30d And 90d Movers")
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
render(recent_movers, limit=10)


heading("6. Consultancy Talking Points")
print(
    """
daily_market_snapshot
- Question answered: Which instruments are under stress, how deep is the drawdown, and how volatile is the recent move?
- Client audience: Risk, asset management, CIO / investment committee.
- Action: Escalate stressed instruments, review hedges, or reassess concentration risk.

fx_trend_signals
- Question answered: Which currency pairs are moving most sharply and is the move broadening or stabilizing?
- Client audience: Treasury, finance, hedging teams.
- Action: Revisit hedge timing, funding assumptions, and exposure commentary.

macro_indicator_trends
- Question answered: Which macro indicators are still improving or deteriorating and how strong is the change?
- Client audience: Strategy, economics, advisory teams.
- Action: Frame client narrative with macro context and likely decision lag.

cross_signal_summary
- Question answered: What is the current cross-domain risk regime and how broad is the stress?
- Client audience: Board reporting, portfolio oversight, advisory leadership.
- Action: Use as a concise top-of-pack risk summary before deeper drill-down.
""".strip()
)


heading("7. Example Business Takeaways")
print(
    """
1. The platform turns three public domains into one advisory-ready risk regime summary, which is more useful than separate siloed datasets.
2. Recent market and FX moves can be discussed together, which is closer to how treasury and risk conversations happen in real client settings.
3. Historical backfill means the Gold layer supports trend, volatility, and drawdown analysis rather than single-day snapshots.
4. The validation layer gives a consulting team confidence that the demo can be repeated without manual data checks.
5. The design is small enough to explain in one meeting, but already structured like a client delivery foundation.
""".strip()
)


heading("8. Why This Is A Credible Databricks Delivery")
print(
    """
Engineering credibility already proven:
- Databricks Asset Bundles deployment
- medallion architecture
- historical backfill plus daily refresh
- deterministic Silver transformations
- Gold analytical tables with clear business semantics
- automated validation checks

What a consultancy could do next:
- add SQL dashboard and board pack views
- extend source coverage with rates or credit signals
- add AI commentary on weekly regime shifts
- productionize with CI, environment promotion, and alerting
""".strip()
)


heading("9. Live Demo Run Order")
print(
    """
Suggested live flow:
1. Mention bootstrap and backfill were run already
2. Show row counts and freshness
3. Open latest cross_signal_summary output
4. Drill into stressed market instruments
5. Show FX signal table
6. Use macro trends to explain why the risk regime matters
7. Close on why Databricks is the right delivery platform
""".strip()
)
