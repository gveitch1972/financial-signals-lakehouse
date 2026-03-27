from datetime import date
import os
import sys

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

if "src" in sys.modules:
    del sys.modules["src"]

import src.gold.build_top_movers_why as top_movers_why


@pytest.fixture(scope="module")
def spark():
    try:
        session = SparkSession.builder.master("local[1]").appName("test-top-movers-why").getOrCreate()
    except Exception as error:
        pytest.skip(f"Spark test session unavailable in this environment: {error}")
    yield session
    session.stop()


def _register_inputs(spark):
    market_rows = [
        (date(2026, 3, 24), "AAA", 100.0, 1.0, 5.0, False),
        (date(2026, 3, 24), "AAB", 101.0, 1.0, 5.0, True),
        (date(2026, 3, 24), "BBB", 92.0, -1.0, -4.0, True),
        (date(2026, 3, 24), "CCC", 95.0, 0.2, 1.5, False),
        (date(2026, 3, 23), "OLD", 88.0, -0.5, -2.0, False),
    ]
    market_df = spark.createDataFrame(
        market_rows,
        ["snapshot_date", "symbol", "latest_price", "day_change_pct", "return_30d_pct", "stress_flag"],
    )
    market_df.createOrReplaceTempView("tmp_gold_market_snapshot")

    fx_rows = [
        (date(2026, 3, 24), "GBPUSD", "strengthening", False),
        (date(2026, 3, 24), "GBPEUR", "weakening", True),
    ]
    fx_df = spark.createDataFrame(fx_rows, ["rate_date", "currency_pair", "trend_signal", "stress_flag"])
    fx_df.createOrReplaceTempView("tmp_gold_fx_trends")

    macro_rows = [
        ("USA", "Population, total", date(2025, 1, 1), "up"),
        ("GBR", "Inflation, consumer prices (annual %)", date(2025, 1, 1), "down"),
    ]
    macro_df = spark.createDataFrame(
        macro_rows,
        ["country_code", "indicator_name", "observation_date", "trend_direction"],
    )
    macro_df.createOrReplaceTempView("tmp_gold_macro_trends")


def test_top_movers_why_schema_and_summary(monkeypatch, spark):
    _register_inputs(spark)
    monkeypatch.setattr(top_movers_why, "GOLD_MARKET_SNAPSHOT", "tmp_gold_market_snapshot")
    monkeypatch.setattr(top_movers_why, "GOLD_FX_TRENDS", "tmp_gold_fx_trends")
    monkeypatch.setattr(top_movers_why, "GOLD_MACRO_TRENDS", "tmp_gold_macro_trends")
    monkeypatch.setattr(top_movers_why, "TOP_MOVERS_PER_DIRECTION", 2)

    result = top_movers_why.build_top_movers_why_df(spark)

    assert result.columns == [
        "as_of_date",
        "symbol",
        "latest_price",
        "day_change_pct",
        "return_30d_pct",
        "stress_flag",
        "fx_context",
        "macro_context",
        "why_summary",
    ]
    assert result.filter("why_summary IS NULL").count() == 0
    assert result.count() == 3


def test_top_movers_why_deterministic_tie_and_missing_context(monkeypatch, spark):
    market_rows = [
        (date(2026, 3, 25), "AAA", 100.0, 1.0, 5.0, False),
        (date(2026, 3, 25), "AAB", 100.0, 1.0, 5.0, False),
        (date(2026, 3, 25), "NEG", 95.0, -0.2, -1.0, True),
    ]
    market_df = spark.createDataFrame(
        market_rows,
        ["snapshot_date", "symbol", "latest_price", "day_change_pct", "return_30d_pct", "stress_flag"],
    )
    market_df.createOrReplaceTempView("tmp_gold_market_snapshot")

    fx_df = spark.createDataFrame([], "rate_date date, currency_pair string, trend_signal string, stress_flag boolean")
    fx_df.createOrReplaceTempView("tmp_gold_fx_trends")

    macro_df = spark.createDataFrame(
        [("USA", "Population, total", date(2027, 1, 1), "up")],
        ["country_code", "indicator_name", "observation_date", "trend_direction"],
    )
    macro_df.createOrReplaceTempView("tmp_gold_macro_trends")

    monkeypatch.setattr(top_movers_why, "GOLD_MARKET_SNAPSHOT", "tmp_gold_market_snapshot")
    monkeypatch.setattr(top_movers_why, "GOLD_FX_TRENDS", "tmp_gold_fx_trends")
    monkeypatch.setattr(top_movers_why, "GOLD_MACRO_TRENDS", "tmp_gold_macro_trends")
    monkeypatch.setattr(top_movers_why, "TOP_MOVERS_PER_DIRECTION", 1)

    result = top_movers_why.build_top_movers_why_df(spark).orderBy("symbol")
    symbols = [row.symbol for row in result.collect()]

    assert symbols == ["AAA", "NEG"]
    assert result.filter("fx_context IS NULL OR macro_context IS NULL OR why_summary IS NULL").count() == 0
