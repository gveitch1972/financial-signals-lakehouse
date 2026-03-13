from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def write_bronze_append(
    df: DataFrame,
    target_table: str,
    source_name: str,
    run_id: str,
) -> None:
    bronze_df = (
        df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
        .withColumn("_source", F.lit(source_name))
        .withColumn("_run_id", F.lit(run_id))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )