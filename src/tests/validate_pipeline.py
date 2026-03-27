from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.audit import log_pipeline_run
from src.common.config import (
    BRONZE_FX_RAW,
    BRONZE_MACRO_RAW,
    BRONZE_MARKET_RAW,
    GOLD_CROSS_SIGNALS,
    GOLD_FX_TRENDS,
    GOLD_MACRO_TRENDS,
    GOLD_MARKET_SNAPSHOT,
    GOLD_TOP_MOVERS_WHY,
    SILVER_FX,
    SILVER_MACRO,
    SILVER_MARKET,
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def scalar(spark: SparkSession, query: str):
    return spark.sql(query).collect()[0][0]


def validate_non_zero_tables(spark: SparkSession):
    tables = [
        BRONZE_MARKET_RAW,
        BRONZE_FX_RAW,
        BRONZE_MACRO_RAW,
        SILVER_MARKET,
        SILVER_FX,
        SILVER_MACRO,
        GOLD_MARKET_SNAPSHOT,
        GOLD_FX_TRENDS,
        GOLD_MACRO_TRENDS,
        GOLD_CROSS_SIGNALS,
        GOLD_TOP_MOVERS_WHY,
    ]
    for table in tables:
        count = scalar(spark, f"SELECT COUNT(*) FROM {table}")
        require(count > 0, f"Validation failed: {table} has zero rows.")


def validate_freshness(spark: SparkSession):
    market_age = scalar(
        spark,
        f"SELECT datediff(current_date(), max(to_date(market_time))) FROM {SILVER_MARKET}",
    )
    fx_age = scalar(
        spark,
        f"SELECT datediff(current_date(), max(rate_date)) FROM {SILVER_FX}",
    )
    macro_age = scalar(
        spark,
        f"SELECT datediff(current_date(), max(observation_date)) FROM {SILVER_MACRO}",
    )
    cross_age = scalar(
        spark,
        f"SELECT datediff(current_date(), max(as_of_date)) FROM {GOLD_CROSS_SIGNALS}",
    )

    require(market_age is not None and market_age <= 10, f"Validation failed: market data freshness is {market_age} days.")
    require(fx_age is not None and fx_age <= 10, f"Validation failed: FX data freshness is {fx_age} days.")
    require(
        macro_age is not None and macro_age <= 1200,
        f"Validation failed: macro data freshness is {macro_age} days.",
    )
    require(cross_age is not None and cross_age <= 10, f"Validation failed: cross-signal freshness is {cross_age} days.")


def validate_duplicates(spark: SparkSession):
    duplicate_checks = {
        SILVER_MARKET: "SELECT COUNT(*) FROM (SELECT symbol, market_time, COUNT(*) c FROM {table} GROUP BY symbol, market_time HAVING COUNT(*) > 1)",
        SILVER_FX: "SELECT COUNT(*) FROM (SELECT base_currency, quote_currency, rate_timestamp, COUNT(*) c FROM {table} GROUP BY base_currency, quote_currency, rate_timestamp HAVING COUNT(*) > 1)",
        SILVER_MACRO: "SELECT COUNT(*) FROM (SELECT country_code, indicator_name, observation_date, COUNT(*) c FROM {table} GROUP BY country_code, indicator_name, observation_date HAVING COUNT(*) > 1)",
    }
    for table, query in duplicate_checks.items():
        duplicate_rows = scalar(spark, query.format(table=table))
        require(duplicate_rows == 0, f"Validation failed: {table} has duplicate business keys.")


def validate_history_depth(spark: SparkSession):
    market_history = scalar(spark, f"SELECT COUNT(DISTINCT to_date(market_time)) FROM {SILVER_MARKET}")
    fx_history = scalar(spark, f"SELECT COUNT(DISTINCT rate_date) FROM {SILVER_FX}")
    macro_history = scalar(spark, f"SELECT COUNT(DISTINCT observation_date) FROM {SILVER_MACRO}")

    require(market_history >= 90, f"Validation failed: market history depth is {market_history} distinct dates; need at least 90.")
    require(fx_history >= 30, f"Validation failed: FX history depth is {fx_history} distinct dates; need at least 30.")
    require(
        macro_history >= 8,
        f"Validation failed: macro history depth is {macro_history} distinct dates; need at least 8.",
    )


def validate_gold_metrics(spark: SparkSession):
    market_missing = scalar(
        spark,
        f"""
        WITH latest_market AS (
            SELECT *,
                   row_number() OVER (PARTITION BY symbol ORDER BY snapshot_date DESC) AS rn
            FROM {GOLD_MARKET_SNAPSHOT}
        )
        SELECT COUNT(*) FROM latest_market
        WHERE rn = 1
          AND (
              return_7d_pct IS NULL
              OR return_30d_pct IS NULL
              OR return_90d_pct IS NULL
              OR rolling_30d_volatility IS NULL
              OR drawdown_from_90d_high_pct IS NULL
          )
        """,
    )
    fx_missing = scalar(
        spark,
        f"""
        WITH latest_fx AS (
            SELECT *,
                   row_number() OVER (PARTITION BY currency_pair ORDER BY rate_date DESC) AS rn,
                   count(*) OVER (PARTITION BY currency_pair) AS pair_history_count
            FROM {GOLD_FX_TRENDS}
        )
        SELECT COUNT(*) FROM latest_fx
        WHERE rn = 1
          AND pair_history_count >= 31
          AND (
              return_30d_pct IS NULL
              OR rolling_30d_volatility IS NULL
              OR trend_signal IS NULL
              OR stress_flag IS NULL
          )
        """,
    )
    cross_missing = scalar(
        spark,
        f"SELECT COUNT(*) FROM {GOLD_CROSS_SIGNALS} WHERE risk_regime IS NULL",
    )
    movers_missing = scalar(
        spark,
        f"SELECT COUNT(*) FROM {GOLD_TOP_MOVERS_WHY} WHERE why_summary IS NULL OR fx_context IS NULL OR macro_context IS NULL",
    )

    require(market_missing == 0, "Validation failed: market Gold metrics are missing on one or more rows.")
    require(fx_missing == 0, "Validation failed: FX Gold metrics are missing on one or more rows.")
    require(cross_missing == 0, "Validation failed: cross-signal risk_regime is missing.")
    require(movers_missing == 0, "Validation failed: top movers context fields are missing.")


def run_validations(spark: SparkSession):
    validate_non_zero_tables(spark)
    validate_freshness(spark)
    validate_duplicates(spark)
    validate_history_depth(spark)
    validate_gold_metrics(spark)

    summary = {
        "status": "SUCCESS",
        "validated_tables": 11,
        "message": "All Bronze, Silver, and Gold validation checks passed, including top movers context.",
    }

    log_pipeline_run(
        spark,
        pipeline_name="validation_queries",
        status=summary["status"],
        row_count=summary["validated_tables"],
        message=summary["message"],
    )
    return summary
