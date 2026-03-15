from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_date, current_timestamp
from delta.tables import DeltaTable

from src.common.config import BRONZE_FX_RAW, SILVER_FX

spark = SparkSession.builder.getOrCreate()

df = spark.read.table(BRONZE_FX_RAW)

clean = (
    df
    .filter(col("rate").isNotNull())
    .dropDuplicates(["base_currency", "quote_currency", "rate_timestamp"])
    .withColumn("currency_pair", concat(col("base_currency"), col("quote_currency")))
    .withColumn("rate", col("rate").cast("decimal(12,5)"))
    .withColumn("rate_date", to_date(col("rate_timestamp")))
    .withColumn("source_system", lit("frankfurter"))
    .withColumn("ingested_at", current_timestamp())
)

merge_condition = """
target.base_currency = source.base_currency
AND target.quote_currency = source.quote_currency
AND target.rate_timestamp = source.rate_timestamp
"""

if spark.catalog.tableExists(SILVER_FX):
    target = DeltaTable.forName(spark, SILVER_FX)

    (
        target.alias("target")
        .merge(clean.alias("source"), merge_condition)
        .whenMatchedUpdate(set={
            "currency_pair": "source.currency_pair",
            "rate": "source.rate",
            "rate_date": "source.rate_date",
            "source_system": "source.source_system",
            "ingested_at": "source.ingested_at"        })
        .whenNotMatchedInsert(values={
            "currency_pair": "source.currency_pair",
            "base_currency": "source.base_currency",
            "quote_currency": "source.quote_currency",
            "rate": "source.rate",
            "rate_timestamp": "source.rate_timestamp",
            "rate_date": "source.rate_date",
            "source_system": "source.source_system",
            "ingested_at": "source.ingested_at"        })
        .execute()
    )
else:
    (
        clean.write
        .format("delta")
        .mode("append")
        .saveAsTable(SILVER_FX)
    )

def main():
    print("MARKER: entering main()")
    clean.printSchema()
    clean.show(truncate=False)

if __name__ == "__main__":
    main()