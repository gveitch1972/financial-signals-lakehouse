from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import (
    GOLD_CROSS_SIGNALS,
    GOLD_FX_TRENDS,
    GOLD_MACRO_TRENDS,
    GOLD_MARKET_SNAPSHOT,
)


def build_macro_asof_summary(base_dates, macro):
    macro_candidates = (
        base_dates.join(
            macro,
            macro.observation_date <= base_dates.as_of_date,
            "left",
        )
        .select(
            base_dates.as_of_date,
            macro.country_code,
            macro.indicator_name,
            macro.observation_date,
            macro.trend_direction,
            macro.period_change_pct,
            macro.year_over_year_pct,
        )
    )

    latest_macro_window = Window.partitionBy(
        "as_of_date",
        "country_code",
        "indicator_name",
    ).orderBy(F.col("observation_date").desc())

    macro_latest_asof = (
        macro_candidates.withColumn("_rn", F.row_number().over(latest_macro_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return (
        macro_latest_asof.groupBy("as_of_date")
        .agg(
            F.sum(F.when(F.col("trend_direction") == "up", 1).otherwise(0)).alias("macro_up_indicators"),
            F.sum(F.when(F.col("trend_direction") == "down", 1).otherwise(0)).alias("macro_down_indicators"),
            F.avg("period_change_pct").alias("macro_avg_period_change_pct"),
            F.avg("year_over_year_pct").alias("macro_avg_year_over_year_pct"),
        )
    )


def build_cross_signal_summary(spark: SparkSession):
    market = spark.read.table(GOLD_MARKET_SNAPSHOT)
    fx = spark.read.table(GOLD_FX_TRENDS)
    macro = spark.read.table(GOLD_MACRO_TRENDS)

    market_summary = (
        market.groupBy(F.col("snapshot_date").alias("as_of_date"))
        .agg(
            F.countDistinct("symbol").alias("market_symbols_count"),
            F.avg("day_change_pct").alias("market_avg_day_change_pct"),
            F.avg("return_30d_pct").alias("market_avg_return_30d_pct"),
            F.avg("return_90d_pct").alias("market_avg_return_90d_pct"),
            F.sum(F.when(F.col("day_change_pct") > 0, 1).otherwise(0)).alias("market_up_symbols"),
            F.sum(F.when(F.col("day_change_pct") < 0, 1).otherwise(0)).alias("market_down_symbols"),
            F.sum(F.when(F.col("stress_flag"), 1).otherwise(0)).alias("market_stress_symbols"),
        )
    )

    fx_summary = (
        fx.groupBy(F.col("rate_date").alias("as_of_date"))
        .agg(
            F.sum(F.when(F.col("trend_signal") == "strengthening", 1).otherwise(0)).alias("fx_strengthening_pairs"),
            F.sum(F.when(F.col("trend_signal") == "weakening", 1).otherwise(0)).alias("fx_weakening_pairs"),
            F.avg("daily_change_pct").alias("fx_avg_daily_change_pct"),
            F.avg("return_30d_pct").alias("fx_avg_return_30d_pct"),
            F.sum(F.when(F.col("stress_flag"), 1).otherwise(0)).alias("fx_stress_pairs"),
        )
    )

    base_dates = (
        market_summary.select("as_of_date")
        .unionByName(fx_summary.select("as_of_date"))
        .dropDuplicates(["as_of_date"])
    )

    macro_summary = build_macro_asof_summary(base_dates, macro)

    result = (
        base_dates.join(market_summary, on="as_of_date", how="left")
        .join(fx_summary, on="as_of_date", how="left")
        .join(macro_summary, on="as_of_date", how="left")
        .fillna(
            {
                "market_symbols_count": 0,
                "market_up_symbols": 0,
                "market_down_symbols": 0,
                "market_stress_symbols": 0,
                "fx_strengthening_pairs": 0,
                "fx_weakening_pairs": 0,
                "fx_stress_pairs": 0,
                "macro_up_indicators": 0,
                "macro_down_indicators": 0,
            }
        )
        .withColumn(
            "risk_regime",
            F.when(
                (F.col("market_avg_return_30d_pct") <= -5.0)
                & ((F.col("fx_weakening_pairs") >= F.col("fx_strengthening_pairs")) | (F.col("fx_stress_pairs") >= 1)),
                F.lit("stress"),
            )
            .when(
                (F.col("market_avg_return_30d_pct") <= -2.0)
                | (F.col("market_stress_symbols") > 0)
                | (F.col("fx_stress_pairs") > 0)
                | (F.col("macro_down_indicators") > F.col("macro_up_indicators")),
                F.lit("elevated"),
            )
            .otherwise(F.lit("calm")),
        )
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
