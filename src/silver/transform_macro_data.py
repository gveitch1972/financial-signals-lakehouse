from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window

from src.common.config import BRONZE_MACRO_RAW, SILVER_MACRO


def main():
    spark = SparkSession.builder.getOrCreate()

    bronze = spark.read.table(BRONZE_MACRO_RAW)

    typed = (
        bronze.withColumn("observation_date", to_date(col("observation_date")))
        .withColumn("observation_value", col("observation_value").cast("double"))
        .filter(
            col("country_code").isNotNull()
            & col("indicator_name").isNotNull()
            & col("observation_date").isNotNull()
            & col("observation_value").isNotNull()
        )
    )

    dedupe_window = Window.partitionBy(
        "country_code", "indicator_name", "observation_date"
    ).orderBy(col("ingest_ts").desc())

    clean = (
        typed.withColumn("_rn", row_number().over(dedupe_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    (
        clean.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_MACRO)
    )


if __name__ == "__main__":
    main()