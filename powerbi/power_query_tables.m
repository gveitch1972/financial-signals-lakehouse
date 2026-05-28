// ============================================================
// Financial Signals Lakehouse — Power Query M table definitions
// Connection: ODBC via Databricks ODBC Driver
//
// SETUP:
//   1. In Power BI Desktop: Home → Transform Data → New Source → ODBC
//   2. Select your Databricks DSN (or enter connection string below)
//   3. For each table below: New Query → Advanced Editor → paste query
//
// DSN connection string format (if not using named DSN):
//   Driver={Simba Spark ODBC Driver};Host=<workspace>.azuredatabricks.net;
//   Port=443;HTTPPath=/sql/1.0/warehouses/<id>;SSL=1;
//   ThriftTransport=2;AuthMech=3;UID=token;PWD=<pat>
// ============================================================


// --- TABLE 1: daily_market_snapshot ---
// Used in: Market Risk Dashboard, Top Movers pages
let
    Source = Odbc.Query(
        "dsn=DatabricksFinSignals",
        "SELECT
            symbol,
            snapshot_date,
            latest_price,
            open_price,
            day_change,
            day_change_pct,
            return_7d_pct,
            return_30d_pct,
            return_90d_pct,
            rolling_30d_volatility,
            drawdown_from_90d_high_pct,
            stress_flag,
            currency,
            market_time
         FROM fin_signals_dev.gold.daily_market_snapshot"
    ),
    #"Changed Types" = Table.TransformColumnTypes(Source, {
        {"snapshot_date", type date},
        {"market_time", type datetime},
        {"latest_price", type number},
        {"open_price", type number},
        {"day_change", type number},
        {"day_change_pct", type number},
        {"return_7d_pct", type number},
        {"return_30d_pct", type number},
        {"return_90d_pct", type number},
        {"rolling_30d_volatility", type number},
        {"drawdown_from_90d_high_pct", type number},
        {"stress_flag", type logical}
    })
in
    #"Changed Types"


// --- TABLE 2: cross_signal_summary ---
// Used in: Regime Timeline, Market Risk Dashboard KPIs
let
    Source = Odbc.Query(
        "dsn=DatabricksFinSignals",
        "SELECT
            as_of_date,
            market_symbols_count,
            market_avg_day_change_pct,
            market_avg_return_30d_pct,
            market_avg_return_90d_pct,
            market_up_symbols,
            market_down_symbols,
            market_stress_symbols,
            fx_strengthening_pairs,
            fx_weakening_pairs,
            fx_avg_daily_change_pct,
            fx_avg_return_30d_pct,
            fx_stress_pairs,
            macro_up_indicators,
            macro_down_indicators,
            macro_avg_period_change_pct,
            macro_avg_year_over_year_pct,
            risk_regime
         FROM fin_signals_dev.gold.cross_signal_summary"
    ),
    #"Changed Types" = Table.TransformColumnTypes(Source, {
        {"as_of_date", type date},
        {"market_symbols_count", Int64.Type},
        {"market_avg_day_change_pct", type number},
        {"market_avg_return_30d_pct", type number},
        {"market_avg_return_90d_pct", type number},
        {"market_up_symbols", Int64.Type},
        {"market_down_symbols", Int64.Type},
        {"market_stress_symbols", Int64.Type},
        {"fx_strengthening_pairs", Int64.Type},
        {"fx_weakening_pairs", Int64.Type},
        {"fx_avg_daily_change_pct", type number},
        {"fx_avg_return_30d_pct", type number},
        {"fx_stress_pairs", Int64.Type},
        {"macro_up_indicators", Int64.Type},
        {"macro_down_indicators", Int64.Type},
        {"macro_avg_period_change_pct", type number},
        {"macro_avg_year_over_year_pct", type number},
        {"risk_regime", type text}
    })
in
    #"Changed Types"


// --- TABLE 3: fx_trend_signals ---
// Used in: FX Signals page
let
    Source = Odbc.Query(
        "dsn=DatabricksFinSignals",
        "SELECT
            currency_pair,
            rate_date,
            base_currency,
            quote_currency,
            rate,
            daily_change,
            daily_change_pct,
            weekly_change_pct,
            return_30d_pct,
            rolling_30d_volatility,
            trend_signal,
            stress_flag
         FROM fin_signals_dev.gold.fx_trend_signals"
    ),
    #"Changed Types" = Table.TransformColumnTypes(Source, {
        {"rate_date", type date},
        {"rate", type number},
        {"daily_change", type number},
        {"daily_change_pct", type number},
        {"weekly_change_pct", type number},
        {"return_30d_pct", type number},
        {"rolling_30d_volatility", type number},
        {"stress_flag", type logical}
    })
in
    #"Changed Types"


// --- TABLE 4: macro_indicator_trends ---
// Used in: Macro Context page
let
    Source = Odbc.Query(
        "dsn=DatabricksFinSignals",
        "SELECT
            country_code,
            indicator_name,
            observation_date,
            observation_value,
            period_change,
            period_change_pct,
            year_over_year_pct,
            trend_direction
         FROM fin_signals_dev.gold.macro_indicator_trends"
    ),
    #"Changed Types" = Table.TransformColumnTypes(Source, {
        {"observation_date", type date},
        {"observation_value", type number},
        {"period_change", type number},
        {"period_change_pct", type number},
        {"year_over_year_pct", type number}
    })
in
    #"Changed Types"


// --- TABLE 5: top_movers_why ---
// Used in: Top Movers page
let
    Source = Odbc.Query(
        "dsn=DatabricksFinSignals",
        "SELECT
            as_of_date,
            symbol,
            latest_price,
            day_change_pct,
            return_30d_pct,
            stress_flag,
            fx_context,
            macro_context,
            why_summary
         FROM fin_signals_dev.gold.top_movers_why"
    ),
    #"Changed Types" = Table.TransformColumnTypes(Source, {
        {"as_of_date", type date},
        {"latest_price", type number},
        {"day_change_pct", type number},
        {"return_30d_pct", type number},
        {"stress_flag", type logical}
    })
in
    #"Changed Types"
