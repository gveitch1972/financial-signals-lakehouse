from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_MARKET_SNAPSHOT, SILVER_MARKET


def build_daily_market_snapshot(spark: SparkSession):
    market = spark.read.table(SILVER_MARKET)

    latest_per_day_window = Window.partitionBy("symbol", "snapshot_date").orderBy(F.col("market_time").desc())
    ordered = Window.partitionBy("symbol").orderBy(F.col("snapshot_date").asc())
    rolling_30 = ordered.rowsBetween(-29, 0)
    rolling_90 = ordered.rowsBetween(-89, 0)

    daily = (
        market.withColumn("snapshot_date", F.to_date("market_time"))
        .withColumn("close_price", F.col("price").cast("double"))
        .filter(F.col("snapshot_date").isNotNull())
        .withColumn("daily_rank", F.row_number().over(latest_per_day_window))
        .filter(F.col("daily_rank") == 1)
        .drop("daily_rank")
    )

    result = (
        daily.withColumn("prev_1_price", F.lag("close_price", 1).over(ordered))
        .withColumn("prev_7_price", F.lag("close_price", 7).over(ordered))
        .withColumn("prev_30_price", F.lag("close_price", 30).over(ordered))
        .withColumn("prev_90_price", F.lag("close_price", 90).over(ordered))
        .withColumn(
            "day_change",
            F.round(F.col("close_price") - F.col("prev_1_price"), 6),
        )
        .withColumn(
            "day_change_pct",
            F.round(
                F.when(F.col("prev_1_price") != 0, ((F.col("close_price") - F.col("prev_1_price")) / F.col("prev_1_price")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "return_7d_pct",
            F.round(
                F.when(F.col("prev_7_price") != 0, ((F.col("close_price") - F.col("prev_7_price")) / F.col("prev_7_price")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "return_30d_pct",
            F.round(
                F.when(F.col("prev_30_price") != 0, ((F.col("close_price") - F.col("prev_30_price")) / F.col("prev_30_price")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "return_90d_pct",
            F.round(
                F.when(F.col("prev_90_price") != 0, ((F.col("close_price") - F.col("prev_90_price")) / F.col("prev_90_price")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "rolling_30d_volatility",
            F.round(F.stddev_samp("day_change_pct").over(rolling_30) * F.sqrt(F.lit(252.0)), 4),
        )
        .withColumn("rolling_90d_high", F.max("close_price").over(rolling_90))
        .withColumn(
            "drawdown_from_90d_high_pct",
            F.round(
                F.when(F.col("rolling_90d_high") != 0, ((F.col("close_price") - F.col("rolling_90d_high")) / F.col("rolling_90d_high")) * 100)
                .otherwise(F.lit(None)),
                4,
            ),
        )
        .withColumn(
            "stress_flag",
            F.coalesce(
                (
                (F.col("day_change_pct") <= F.lit(-2.0))
                | (F.col("return_30d_pct") <= F.lit(-5.0))
                | (F.col("rolling_30d_volatility") >= F.lit(25.0))
                | (F.col("drawdown_from_90d_high_pct") <= F.lit(-10.0))
                ),
                F.lit(False),
            ),
        )
        .select(
            "symbol",
            "snapshot_date",
            F.col("close_price").alias("latest_price"),
            F.col("prev_1_price").alias("open_price"),
            "day_change",
            "day_change_pct",
            "return_7d_pct",
            "return_30d_pct",
            "return_90d_pct",
            "rolling_30d_volatility",
            "drawdown_from_90d_high_pct",
            "stress_flag",
            "currency",
            "market_time",
            "ingested_at",
        )
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_MARKET_SNAPSHOT)
    )

    return result


def main():
    spark = SparkSession.builder.getOrCreate()
    result = build_daily_market_snapshot(spark)
    result.printSchema()
    result.show(truncate=False)


if __name__ == "__main__":
    main()
