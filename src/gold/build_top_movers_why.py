from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import (
    GOLD_FX_TRENDS,
    GOLD_MACRO_TRENDS,
    GOLD_MARKET_SNAPSHOT,
    GOLD_TOP_MOVERS_WHY,
)

TOP_MOVERS_PER_DIRECTION = 5


def _build_fx_context(latest_date, fx):
    fx_agg = (
        fx.join(latest_date, fx.rate_date == latest_date.as_of_date, "inner")
        .groupBy()
        .agg(
            F.sum(F.when(F.col("trend_signal") == "strengthening", 1).otherwise(0)).alias("fx_strengthening_pairs"),
            F.sum(F.when(F.col("trend_signal") == "weakening", 1).otherwise(0)).alias("fx_weakening_pairs"),
            F.sum(F.when(F.col("stress_flag"), 1).otherwise(0)).alias("fx_stress_pairs"),
        )
    )

    return (
        latest_date.crossJoin(fx_agg)
        .select(
            "as_of_date",
            F.concat(
                F.lit("FX context: "),
                F.coalesce(F.col("fx_strengthening_pairs"), F.lit(0)).cast("int"),
                F.lit(" strengthening, "),
                F.coalesce(F.col("fx_weakening_pairs"), F.lit(0)).cast("int"),
                F.lit(" weakening, "),
                F.coalesce(F.col("fx_stress_pairs"), F.lit(0)).cast("int"),
                F.lit(" stressed pairs."),
            ).alias("fx_context"),
        )
    )


def _build_macro_context(latest_date, macro):
    macro_candidates = (
        macro.join(latest_date, macro.observation_date <= latest_date.as_of_date, "inner")
        .select(
            latest_date.as_of_date.alias("as_of_date"),
            macro.country_code,
            macro.indicator_name,
            macro.observation_date,
            macro.trend_direction,
        )
    )

    latest_macro_window = Window.partitionBy("country_code", "indicator_name").orderBy(F.col("observation_date").desc())

    macro_latest = (
        macro_candidates.withColumn("_rn", F.row_number().over(latest_macro_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    macro_agg = macro_latest.groupBy().agg(
        F.sum(F.when(F.col("trend_direction") == "up", 1).otherwise(0)).alias("macro_up_indicators"),
        F.sum(F.when(F.col("trend_direction") == "down", 1).otherwise(0)).alias("macro_down_indicators"),
    )

    return (
        latest_date.crossJoin(macro_agg)
        .select(
            "as_of_date",
            F.concat(
                F.lit("Macro context: "),
                F.coalesce(F.col("macro_up_indicators"), F.lit(0)).cast("int"),
                F.lit(" up indicators, "),
                F.coalesce(F.col("macro_down_indicators"), F.lit(0)).cast("int"),
                F.lit(" down indicators."),
            ).alias("macro_context"),
        )
    )


def build_top_movers_why_df(spark: SparkSession):
    market = spark.read.table(GOLD_MARKET_SNAPSHOT)
    fx = spark.read.table(GOLD_FX_TRENDS)
    macro = spark.read.table(GOLD_MACRO_TRENDS)

    latest_date = market.select(F.max("snapshot_date").alias("as_of_date")).filter(F.col("as_of_date").isNotNull())

    latest_market = (
        market.join(latest_date, market.snapshot_date == latest_date.as_of_date, "inner")
        .select(
            latest_date.as_of_date.alias("as_of_date"),
            F.col("symbol"),
            F.col("latest_price"),
            F.col("day_change_pct"),
            F.col("return_30d_pct"),
            F.col("stress_flag"),
        )
        .filter(F.col("return_30d_pct").isNotNull())
    )

    up_window = Window.orderBy(
        F.col("return_30d_pct").desc(),
        F.col("day_change_pct").desc(),
        F.col("symbol").asc(),
    )
    down_window = Window.orderBy(
        F.col("return_30d_pct").asc(),
        F.col("day_change_pct").asc(),
        F.col("symbol").asc(),
    )

    top_up = (
        latest_market.filter(F.col("return_30d_pct") > 0)
        .withColumn("_rank", F.row_number().over(up_window))
        .filter(F.col("_rank") <= F.lit(TOP_MOVERS_PER_DIRECTION))
        .drop("_rank")
    )

    top_down = (
        latest_market.filter(F.col("return_30d_pct") < 0)
        .withColumn("_rank", F.row_number().over(down_window))
        .filter(F.col("_rank") <= F.lit(TOP_MOVERS_PER_DIRECTION))
        .drop("_rank")
    )

    movers = top_up.unionByName(top_down).dropDuplicates(["as_of_date", "symbol"])

    fx_context = _build_fx_context(latest_date, fx)
    macro_context = _build_macro_context(latest_date, macro)

    return (
        movers.join(fx_context, on="as_of_date", how="left")
        .join(macro_context, on="as_of_date", how="left")
        .withColumn(
            "why_summary",
            F.concat_ws(
                " ",
                F.when(F.col("stress_flag"), F.lit("Market stress flag is active."))
                .otherwise(F.lit("Market stress flag is not active.")),
                F.when(F.col("return_30d_pct") <= -5.0, F.lit("30d return indicates material downside pressure."))
                .when(F.col("return_30d_pct") >= 5.0, F.lit("30d return indicates strong positive momentum."))
                .otherwise(F.lit("30d return is directionally moderate.")),
                F.coalesce(F.col("fx_context"), F.lit("FX context unavailable.")),
                F.coalesce(F.col("macro_context"), F.lit("Macro context unavailable.")),
            ),
        )
        .select(
            "as_of_date",
            "symbol",
            "latest_price",
            "day_change_pct",
            "return_30d_pct",
            "stress_flag",
            "fx_context",
            "macro_context",
            "why_summary",
        )
        .orderBy(F.col("return_30d_pct").asc(), F.col("symbol").asc())
    )


def build_top_movers_why(spark: SparkSession):
    result = build_top_movers_why_df(spark)
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TOP_MOVERS_WHY)
    )
    return result


def main():
    spark = SparkSession.builder.getOrCreate()
    result = build_top_movers_why(spark)
    result.printSchema()
    result.show(truncate=False)


if __name__ == "__main__":
    main()
