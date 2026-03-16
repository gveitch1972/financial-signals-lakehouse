from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, round, when
from pyspark.sql.window import Window

from src.common.config import GOLD_MACRO_TRENDS, SILVER_MACRO


def main():
    spark = SparkSession.builder.getOrCreate()

    macro = spark.read.table(SILVER_MACRO)

    trend_window = Window.partitionBy("country_code", "indicator_name").orderBy(
        col("observation_date")
    )

    with_prev = macro.withColumn("prev_value", lag("observation_value").over(trend_window))

    trends = (
        with_prev.withColumn("change_value", col("observation_value") - col("prev_value"))
        .withColumn(
            "change_pct",
            when(col("prev_value").isNull() | (col("prev_value") == 0), None).otherwise(
                round((col("change_value") / col("prev_value")) * 100, 4)
            ),
        )
        .withColumn(
            "trend_direction",
            when(col("change_value") > 0, "increase")
            .when(col("change_value") < 0, "decrease")
            .otherwise("flat"),
        )
    )

    (
        trends.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_MACRO_TRENDS)
    )


if __name__ == "__main__":
    main()