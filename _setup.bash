mkdir -p src/common src/bronze src/silver src/gold src/tests
mkdir -p notebooks resources/jobs resources/pipelines sample_data
touch .gitignore CHANGELOG.md LICENSE
touch src/common/config.py src/common/logging_utils.py src/common/schema_utils.py src/common/audit.py
touch src/bronze/ingest_market_data.py src/bronze/ingest_fx_data.py src/bronze/ingest_macro_data.py
touch src/silver/transform_market_data.py src/silver/transform_fx_data.py src/silver/transform_macro_data.py
touch src/gold/build_daily_market_snapshot.py src/gold/build_fx_trend_signals.py src/gold/build_macro_indicator_trends.py src/gold/build_cross_signal_summary.py
touch src/tests/test_config.py src/tests/test_transforms.py
touch notebooks/01_bronze_ingestion_demo.py notebooks/02_silver_transform_demo.py notebooks/03_gold_analytics_demo.py notebooks/99_validation_queries.py
touch resources/jobs/daily_pipeline_job.yml resources/pipelines/medallion_pipeline_notes.md
touch sample_data/README.md
