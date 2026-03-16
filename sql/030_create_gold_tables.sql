CREATE TABLE IF NOT EXISTS fin_signals_dev.gold.daily_market_snapshot (
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
USING DELTA;

CREATE TABLE IF NOT EXISTS fin_signals_dev.gold.fx_trend_signals (
    currency_pair STRING,
    rate_date DATE,
    base_currency STRING,
    quote_currency STRING,
    rate DOUBLE,
    daily_change DOUBLE,
    daily_change_pct DOUBLE,
    weekly_change_pct DOUBLE,
    return_30d_pct DOUBLE,
    rolling_30d_volatility DOUBLE,
    trend_signal STRING,
    stress_flag BOOLEAN,
    ingested_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS fin_signals_dev.gold.macro_indicator_trends (
    country_code STRING,
    indicator_name STRING,
    observation_date DATE,
    observation_value DOUBLE,
    period_change DOUBLE,
    period_change_pct DOUBLE,
    year_over_year_pct DOUBLE,
    trend_direction STRING,
    ingested_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS fin_signals_dev.gold.cross_signal_summary (
    as_of_date DATE,
    market_symbols_count BIGINT,
    market_avg_day_change_pct DOUBLE,
    market_avg_return_30d_pct DOUBLE,
    market_avg_return_90d_pct DOUBLE,
    market_up_symbols BIGINT,
    market_down_symbols BIGINT,
    market_stress_symbols BIGINT,
    fx_strengthening_pairs BIGINT,
    fx_weakening_pairs BIGINT,
    fx_avg_daily_change_pct DOUBLE,
    fx_avg_return_30d_pct DOUBLE,
    fx_stress_pairs BIGINT,
    macro_up_indicators BIGINT,
    macro_down_indicators BIGINT,
    macro_avg_period_change_pct DOUBLE,
    macro_avg_year_over_year_pct DOUBLE,
    risk_regime STRING
)
USING DELTA;
