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