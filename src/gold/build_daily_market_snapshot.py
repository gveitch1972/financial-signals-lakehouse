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
            "ingested_at",
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