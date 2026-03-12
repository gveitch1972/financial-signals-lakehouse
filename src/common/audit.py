from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from .config import AUDIT_PIPELINE_RUNS


def log_pipeline_run(
    spark: SparkSession,
    pipeline_name: str,
    status: str,
    row_count: int = None,
    message: str = None
):
    """
    Write a pipeline run record to the audit table.
    """

    data = [{
        "pipeline_name": pipeline_name,
        "status": status,
        "row_count": row_count,
        "message": message,
        "run_timestamp": datetime.utcnow()
    }]

    df = spark.createDataFrame(data)

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_PIPELINE_RUNS)
    )