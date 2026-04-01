# Databricks notebook source
import json
from datetime import date, datetime
from decimal import Decimal


def _serialize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _rows(query: str):
    result = spark.sql(query).collect()
    return [{k: _serialize(v) for k, v in row.asDict().items()} for row in result]


def _flag(name: str, default: bool = False) -> bool:
    try:
        raw = dbutils.widgets.get(name)
    except Exception:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


include_heavy = _flag("include_heavy", default=False)

payload = {
    "latest_dates": _rows(
        """
        SELECT
            (SELECT CAST(MAX(as_of_date) AS STRING) FROM fin_signals_dev.gold.cross_signal_summary) AS regime_as_of_date,
            (SELECT CAST(MAX(snapshot_date) AS STRING) FROM fin_signals_dev.gold.daily_market_snapshot) AS market_snapshot_date,
            (SELECT CAST(MAX(rate_date) AS STRING) FROM fin_signals_dev.gold.fx_trend_signals) AS fx_rate_date,
            (SELECT CAST(MAX(as_of_date) AS STRING) FROM fin_signals_dev.gold.top_movers_why) AS movers_as_of_date
        """
    )[0],
    "regime": _rows(
        """
        SELECT
            as_of_date,
            risk_regime,
            market_stress_symbols,
            market_up_symbols,
            market_down_symbols,
            fx_stress_pairs,
            fx_strengthening_pairs,
            fx_weakening_pairs,
            macro_up_indicators,
            macro_down_indicators,
            market_avg_day_change_pct,
            market_avg_return_30d_pct,
            fx_avg_daily_change_pct,
            fx_avg_return_30d_pct,
            macro_avg_period_change_pct,
            macro_avg_year_over_year_pct
        FROM fin_signals_dev.gold.cross_signal_summary
        ORDER BY as_of_date DESC
        LIMIT 1
        """
    ),
    "top_movers": _rows(
        """
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
        FROM fin_signals_dev.gold.top_movers_why
        WHERE as_of_date = (
            SELECT MAX(as_of_date)
            FROM fin_signals_dev.gold.top_movers_why
        )
        ORDER BY return_30d_pct ASC, symbol ASC
        LIMIT 10
        """
    ),
    "market_breadth": _rows(
        """
        WITH latest_snapshot AS (
            SELECT *
            FROM fin_signals_dev.gold.daily_market_snapshot
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM fin_signals_dev.gold.daily_market_snapshot
            )
        )
        SELECT
            snapshot_date,
            COUNT(*) AS symbols_in_snapshot,
            SUM(CASE WHEN day_change_pct > 0 THEN 1 ELSE 0 END) AS advancing_symbols,
            SUM(CASE WHEN day_change_pct < 0 THEN 1 ELSE 0 END) AS declining_symbols,
            SUM(CASE WHEN stress_flag THEN 1 ELSE 0 END) AS stressed_symbols,
            ROUND(AVG(day_change_pct), 4) AS avg_day_change_pct,
            ROUND(AVG(return_30d_pct), 4) AS avg_return_30d_pct,
            ROUND(AVG(return_90d_pct), 4) AS avg_return_90d_pct,
            ROUND(AVG(rolling_30d_volatility), 4) AS avg_rolling_30d_volatility
        FROM latest_snapshot
        GROUP BY snapshot_date
        """
    ),
    "market_stress": _rows(
        """
        SELECT
            symbol,
            snapshot_date,
            latest_price,
            day_change_pct,
            return_30d_pct,
            return_90d_pct,
            rolling_30d_volatility,
            drawdown_from_90d_high_pct,
            stress_flag,
            currency
        FROM fin_signals_dev.gold.daily_market_snapshot
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM fin_signals_dev.gold.daily_market_snapshot
        )
        ORDER BY
            stress_flag DESC,
            drawdown_from_90d_high_pct ASC,
            rolling_30d_volatility DESC
        LIMIT 10
        """
    ),
    "fx_watchlist": _rows(
        """
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
        FROM fin_signals_dev.gold.fx_trend_signals
        WHERE rate_date = (
            SELECT MAX(rate_date)
            FROM fin_signals_dev.gold.fx_trend_signals
        )
        ORDER BY
            stress_flag DESC,
            ABS(return_30d_pct) DESC,
            rolling_30d_volatility DESC
        LIMIT 10
        """
    ),
    "macro_trends": _rows(
        """
        SELECT
            country_code,
            indicator_name,
            observation_date,
            observation_value,
            period_change_pct,
            year_over_year_pct,
            trend_direction
        FROM fin_signals_dev.gold.macro_indicator_trends
        WHERE observation_date = (
            SELECT MAX(observation_date)
            FROM fin_signals_dev.gold.macro_indicator_trends
        )
        ORDER BY
            observation_date DESC,
            country_code,
            indicator_name
        LIMIT 15
        """
    ),
}

if include_heavy:
    payload["heavy"] = {
        "market_coverage": _rows(
            """
            SELECT
                MIN(snapshot_date) AS first_snapshot_date,
                MAX(snapshot_date) AS latest_snapshot_date,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT symbol) AS distinct_symbols,
                COUNT(DISTINCT snapshot_date) AS distinct_snapshot_dates,
                COUNT(DISTINCT currency) AS distinct_currencies
            FROM fin_signals_dev.gold.daily_market_snapshot
            """
        ),
        "fx_coverage": _rows(
            """
            SELECT
                MIN(rate_date) AS first_rate_date,
                MAX(rate_date) AS latest_rate_date,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT currency_pair) AS distinct_currency_pairs,
                COUNT(DISTINCT rate_date) AS distinct_rate_dates
            FROM fin_signals_dev.gold.fx_trend_signals
            """
        ),
        "fx_history_summary": _rows(
            """
            WITH pair_history AS (
                SELECT
                    currency_pair,
                    MIN(rate_date) AS first_rate_date,
                    MAX(rate_date) AS latest_rate_date,
                    COUNT(*) AS observations,
                    AVG(ABS(daily_change_pct)) AS avg_abs_daily_change_pct,
                    AVG(rolling_30d_volatility) AS avg_rolling_30d_volatility,
                    MAX(ABS(return_30d_pct)) AS max_abs_return_30d_pct
                FROM fin_signals_dev.gold.fx_trend_signals
                GROUP BY currency_pair
            )
            SELECT
                currency_pair,
                first_rate_date,
                latest_rate_date,
                observations,
                ROUND(avg_abs_daily_change_pct, 4) AS avg_abs_daily_change_pct,
                ROUND(avg_rolling_30d_volatility, 4) AS avg_rolling_30d_volatility,
                ROUND(max_abs_return_30d_pct, 4) AS max_abs_return_30d_pct
            FROM pair_history
            ORDER BY
                observations DESC,
                avg_rolling_30d_volatility DESC,
                currency_pair
            LIMIT 10
            """
        ),
        "macro_coverage": _rows(
            """
            SELECT
                MIN(observation_date) AS first_observation_date,
                MAX(observation_date) AS latest_observation_date,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT country_code) AS distinct_countries,
                COUNT(DISTINCT indicator_name) AS distinct_indicators,
                COUNT(DISTINCT observation_date) AS distinct_observation_dates
            FROM fin_signals_dev.gold.macro_indicator_trends
            """
        ),
    }

dbutils.notebook.exit(json.dumps(payload))
