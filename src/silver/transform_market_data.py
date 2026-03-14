from pyspark.sql.functions import col
from pyspark.sql import SparkSession

from src.common.config import BRONZE_MARKET_RAW
from src.common.config import SILVER_MARKET

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "fin_signals_dev.bronze.market_raw"
SILVER_TABLE = "fin_signals_dev.silver.market_prices"

df = spark.read.table(BRONZE_MARKET_RAW)

clean = (
    df
    .filter(col("price").isNotNull())
    .dropDuplicates(["symbol", "market_time"])
)

clean = clean.withColumn(
    "price",
    col("price").cast("decimal(12,4)")
)

(
    clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_MARKET)
)

def main():
    print("MARKER: entering main()")

    clean.printSchema()
    clean.show(truncate=False)
    

if __name__ == "__main__":
    main()


