from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import (
    GOLD_CROSS_SIGNALS,
    GOLD_FX_TRENDS,
    GOLD_MACRO_TRENDS,
    GOLD_MARKET_SNAPSHOT,
)


def _latest_by_date(df, partition_cols, date_col):
    window = Window.partitionBy(*partition_cols).orderBy(F.col(date_col).desc())
    return df.withColumn("_rk", F.row_number().over(window)).filter(F.col("_rk") == 1).drop("_rk")


def build_cross_signal_summary(spark: SparkSession):
    market_latest = _latest_by_date(
        spark.read.table(GOLD_MARKET_SNAPSHOT),
        ["symbol"],
        "snapshot_date",
    ).select(
        "symbol",
        F.col("snapshot_date").alias("as_of_date"),
        "day_change_pct",
        F.col("currency").alias("market_currency"),
    )

    fx_latest = _latest_by_date(
        spark.read.table(GOLD_FX_TRENDS),
        ["currency_pair"],
        "rate_date",
    ).select(
        "currency_pair",
        F.col("rate_date").alias("as_of_date"),
        "trend_signal",
        "daily_change_pct",
        "weekly_change_pct",
    )

    macro_latest = _latest_by_date(
        spark.read.table(GOLD_MACRO_TRENDS),
        ["country_code", "indicator_name"],
        "observation_date",
    ).select(
        "country_code",
        "indicator_name",
        F.col("observation_date").alias("as_of_date"),
        "trend_direction",
        "period_change_pct",
        "year_over_year_pct",
    )

    fx_summary = (
        fx_latest.groupBy("as_of_date")
        .agg(
            F.sum(F.when(F.col("trend_signal") == "strengthening", 1).otherwise(0)).alias("fx_strengthening_pairs"),
            F.sum(F.when(F.col("trend_signal") == "weakening", 1).otherwise(0)).alias("fx_weakening_pairs"),
            F.avg("daily_change_pct").alias("fx_avg_daily_change_pct"),
        )
    )

    macro_summary = (
        macro_latest.groupBy("as_of_date")
        .agg(
            F.sum(F.when(F.col("trend_direction") == "up", 1).otherwise(0)).alias("macro_up_indicators"),
            F.sum(F.when(F.col("trend_direction") == "down", 1).otherwise(0)).alias("macro_down_indicators"),
            F.avg("period_change_pct").alias("macro_avg_period_change_pct"),
        )
    )

    market_summary = (
        market_latest.groupBy("as_of_date")
        .agg(
            F.countDistinct("symbol").alias("market_symbols_count"),
            F.avg("day_change_pct").alias("market_avg_day_change_pct"),
            F.sum(F.when(F.col("day_change_pct") > 0, 1).otherwise(0)).alias("market_up_symbols"),
            F.sum(F.when(F.col("day_change_pct") < 0, 1).otherwise(0)).alias("market_down_symbols"),
        )
    )

    result = (
        market_summary.join(fx_summary, on="as_of_date", how="full")
        .join(macro_summary, on="as_of_date", how="full")
        .orderBy(F.col("as_of_date").desc())
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_CROSS_SIGNALS)
    )

    return result


def main():
    spark = SparkSession.builder.getOrCreate()
    result = build_cross_signal_summary(spark)
    result.printSchema()
    result.show(truncate=False)


if __name__ == "__main__":
    main()
src/gold/build_daily_market_snapshot.py
src/gold/build_daily_market_snapshot.py
+62
-0

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_MARKET_SNAPSHOT, SILVER_MARKET


def build_daily_market_snapshot(spark: SparkSession):
    market = spark.read.table(SILVER_MARKET)

    daily = (
        market.withColumn("snapshot_date", F.to_date("market_time"))
        .withColumn("close_price", F.col("price").cast("double"))
        .filter(F.col("snapshot_date").isNotNull())
    )

    symbol_day_window = Window.partitionBy("symbol", "snapshot_date").orderBy(F.col("market_time").asc())
    latest_per_day_window = Window.partitionBy("symbol", "snapshot_date").orderBy(F.col("market_time").desc())

    enriched = (
        daily.withColumn("open_price", F.first("close_price").over(symbol_day_window))
        .withColumn("daily_rank", F.row_number().over(latest_per_day_window))
    )

    snapshot = (
        enriched.filter(F.col("daily_rank") == 1)
        .select(
            "symbol",
            "snapshot_date",
            F.col("close_price").alias("latest_price"),
            "open_price",
            F.round(F.col("close_price") - F.col("open_price"), 6).alias("day_change"),
            F.round(
                F.when(F.col("open_price") != 0, ((F.col("close_price") - F.col("open_price")) / F.col("open_price")) * 100)
                .otherwise(F.lit(None)),
                4,
            ).alias("day_change_pct"),
            "currency",
            "market_time",
            "ingest_ts",
        )
    )

    (
        snapshot.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_MARKET_SNAPSHOT)
    )

    return snapshot


def main():
    spark = SparkSession.builder.getOrCreate()
    result = build_daily_market_snapshot(spark)
    result.printSchema()
    result.show(truncate=False)


if __name__ == "__main__":
    main()
src/gold/build_fx_trend_signals.py
src/gold/build_fx_trend_signals.py
+72
-0

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_FX_TRENDS, SILVER_FX


def build_fx_trend_signals(spark: SparkSession):
    fx = spark.read.table(SILVER_FX)

    ordered = Window.partitionBy("currency_pair").orderBy(F.col("rate_date").asc())

    result = (
        fx.withColumn("rate_value", F.col("exchange_rate").cast("double"))
        .withColumn("prev_rate", F.lag("rate_value", 1).over(ordered))
        .withColumn("prev_5_rate", F.lag("rate_value", 5).over(ordered))
        .withColumn("daily_change", F.round(F.col("rate_value") - F.col("prev_rate"), 8))
        .withColumn(
            "daily_change_pct",
            F.round(
                F.when(F.col("prev_rate") != 0, ((F.col("rate_value") - F.col("prev_rate")) / F.col("prev_rate")) * 100)
                .otherwise(F.lit(None)),
                6,
            ),
        )
        .withColumn(
            "weekly_change_pct",
            F.round(
                F.when(F.col("prev_5_rate") != 0, ((F.col("rate_value") - F.col("prev_5_rate")) / F.col("prev_5_rate")) * 100)
                .otherwise(F.lit(None)),
                6,
            ),
        )
        .withColumn(
            "trend_signal",
            F.when(F.col("daily_change_pct") >= 0.25, F.lit("strengthening"))
            .when(F.col("daily_change_pct") <= -0.25, F.lit("weakening"))
            .otherwise(F.lit("stable")),
        )
        .select(
            "currency_pair",
            "rate_date",
            "base_currency",
            "quote_currency",
            F.col("rate_value").alias("exchange_rate"),
            "daily_change",
            "daily_change_pct",
            "weekly_change_pct",
            "trend_signal",
            "ingest_ts",
        )
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_FX_TRENDS)
    )

    return result


def main():
    spark = SparkSession.builder.getOrCreate()
    result = build_fx_trend_signals(spark)
    result.printSchema()
    result.show(truncate=False)


if __name__ == "__main__":
    main()
src/gold/build_macro_indicator_trends.py
src/gold/build_macro_indicator_trends.py
+71
-0

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_MACRO_TRENDS, SILVER_MACRO


def build_macro_indicator_trends(spark: SparkSession):
    macro = spark.read.table(SILVER_MACRO)

    ordered = Window.partitionBy("country_code", "indicator_name").orderBy(F.col("observation_date").asc())

    result = (
        macro.withColumn("obs_value", F.col("observation_value").cast("double"))
        .withColumn("prev_obs", F.lag("obs_value", 1).over(ordered))
        .withColumn("prev_12_obs", F.lag("obs_value", 12).over(ordered))
        .withColumn("period_change", F.round(F.col("obs_value") - F.col("prev_obs"), 6))
        .withColumn(
            "period_change_pct",
            F.round(
                F.when(F.col("prev_obs") != 0, ((F.col("obs_value") - F.col("prev_obs")) / F.col("prev_obs")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "year_over_year_pct",
            F.round(
                F.when(F.col("prev_12_obs") != 0, ((F.col("obs_value") - F.col("prev_12_obs")) / F.col("prev_12_obs")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "trend_direction",
            F.when(F.col("period_change") > 0, F.lit("up"))
            .when(F.col("period_change") < 0, F.lit("down"))
            .otherwise(F.lit("flat")),
        )
        .select(
            "country_code",
            "indicator_name",
            "observation_date",
            F.col("obs_value").alias("observation_value"),
            "period_change",
            "period_change_pct",
            "year_over_year_pct",
            "trend_direction",
            "ingest_ts",
        )
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_MACRO_TRENDS)
    )

    return result


def main():
    spark = SparkSession.builder.getOrCreate()
    result = build_macro_indicator_trends(spark)
    result.printSchema()
    result.show(truncate=False)


if __name__ == "__main__":
    main()
