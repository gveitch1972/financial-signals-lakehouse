"""
Project configuration and naming conventions.
Centralised so all modules reference the same catalog/table names.
"""

# Catalogs
CATALOG = "fin_signals_dev"

# Schemas
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
AUDIT_SCHEMA = "audit"

# Bronze tables
BRONZE_MARKET_RAW = f"{CATALOG}.{BRONZE_SCHEMA}.market_prices_raw"
BRONZE_FX_RAW = f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw"
BRONZE_MACRO_RAW = f"{CATALOG}.{BRONZE_SCHEMA}.macro_indicators_raw"

# Silver tables
SILVER_MARKET = f"{CATALOG}.{SILVER_SCHEMA}.market_prices"
SILVER_FX = f"{CATALOG}.{SILVER_SCHEMA}.fx_rates"
SILVER_MACRO = f"{CATALOG}.{SILVER_SCHEMA}.macro_indicators"

# Gold tables
GOLD_MARKET_SNAPSHOT = f"{CATALOG}.{GOLD_SCHEMA}.daily_market_snapshot"
GOLD_FX_TRENDS = f"{CATALOG}.{GOLD_SCHEMA}.fx_trend_signals"
GOLD_MACRO_TRENDS = f"{CATALOG}.{GOLD_SCHEMA}.macro_indicator_trends"
GOLD_CROSS_SIGNALS = f"{CATALOG}.{GOLD_SCHEMA}.cross_signal_summary"
GOLD_TOP_MOVERS_WHY = f"{CATALOG}.{GOLD_SCHEMA}.top_movers_why"

# Audit tables
AUDIT_PIPELINE_RUNS = f"{CATALOG}.{AUDIT_SCHEMA}.pipeline_runs"
AUDIT_INGEST_EVENTS = f"{CATALOG}.{AUDIT_SCHEMA}.ingest_events"
