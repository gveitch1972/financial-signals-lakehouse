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
