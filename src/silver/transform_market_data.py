from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.audit import log_pipeline_run
from src.common.config import BRONZE_MARKET_RAW, SILVER_MARKET


def build_clean_market_df(spark: SparkSession):
    bronze = spark.read.table(BRONZE_MARKET_RAW)

    typed = (
        bronze
        .withColumn("price", F.col("price").cast("decimal(12,4)"))
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("market_time").isNotNull())
        .filter(F.col("price").isNotNull())
    )

    dedupe_window = Window.partitionBy("symbol", "market_time").orderBy(F.col("ingested_at").desc())

    return (
        typed.withColumn("_rn", F.row_number().over(dedupe_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select("symbol", "price", "currency", "market_time", "ingested_at", "_ingest_date", "_source")
    )


def safe_log_pipeline_run(spark, pipeline_name, status, row_count, message=None):
    try:
        log_pipeline_run(
            spark,
            pipeline_name=pipeline_name,
            status=status,
            row_count=row_count,
            message=message,
        )
    except Exception as error:
        print(f"Audit logging skipped due to error: {error}")


def main():
    spark = SparkSession.builder.getOrCreate()
    try:
        clean = build_clean_market_df(spark)

        (
            clean.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(SILVER_MARKET)
        )

        row_count = clean.count()
        safe_log_pipeline_run(
            spark,
            pipeline_name="silver_market_transform",
            status="SUCCESS",
            row_count=row_count,
            message="silver market transform completed",
        )

        clean.printSchema()
        clean.show(truncate=False)
    except Exception as error:
        safe_log_pipeline_run(
            spark,
            pipeline_name="silver_market_transform",
            status="FAILED",
            row_count=0,
            message=str(error),
        )
        raise


if __name__ == "__main__":
    main()
