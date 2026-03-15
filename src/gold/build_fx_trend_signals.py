from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_FX_TRENDS, SILVER_FX


def build_fx_trend_signals(spark: SparkSession):
    fx = spark.read.table(SILVER_FX)

    ordered = Window.partitionBy("currency_pair").orderBy(F.col("rate_date").asc())

    result = (
        fx.withColumn("rate_value", F.col("rate").cast("double"))
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
            F.col("rate_value").alias("rate"),
            "daily_change",
            "daily_change_pct",
            "weekly_change_pct",
            "trend_signal",
            "ingested_at",
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