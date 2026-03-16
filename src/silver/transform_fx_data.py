from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import BRONZE_FX_RAW, SILVER_FX


def build_clean_fx_df(spark: SparkSession):
    bronze = spark.read.table(BRONZE_FX_RAW)

    typed = (
        bronze
        .filter(F.col("base_currency").isNotNull())
        .filter(F.col("quote_currency").isNotNull())
        .filter(F.col("rate_timestamp").isNotNull())
        .filter(F.col("rate").isNotNull())
        .withColumn("currency_pair", F.concat(F.col("base_currency"), F.col("quote_currency")))
        .withColumn("rate", F.col("rate").cast("decimal(12,5)"))
        .withColumn("rate_date", F.to_date(F.col("rate_timestamp")))
        .withColumn("source_system", F.coalesce(F.col("source_name"), F.lit("frankfurter")))
    )

    dedupe_window = Window.partitionBy(
        "base_currency",
        "quote_currency",
        "rate_timestamp",
    ).orderBy(F.col("ingested_at").desc())

    return (
        typed.withColumn("_rn", F.row_number().over(dedupe_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "currency_pair",
            "base_currency",
            "quote_currency",
            "rate",
            "rate_timestamp",
            "rate_date",
            "source_system",
            "ingested_at",
        )
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    clean = build_clean_fx_df(spark)

    (
        clean.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_FX)
    )

    clean.printSchema()
    clean.show(truncate=False)


if __name__ == "__main__":
    main()
