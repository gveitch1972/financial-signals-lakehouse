from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

from src.common.config import AUDIT_PIPELINE_RUNS

audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("message", StringType(), True),
    StructField("run_timestamp", TimestampType(), True),
])

def log_pipeline_run(spark, pipeline_name, status, row_count, message=None):
    data = [{
        "pipeline_name": pipeline_name,
        "status": status,
        "row_count": int(row_count) if row_count is not None else None,
        "message": message,
        "run_timestamp": datetime.utcnow(),
    }]

    df = spark.createDataFrame(data, schema=audit_schema)

    (
        df.write
          .format("delta")
          .mode("append")
          .saveAsTable(AUDIT_PIPELINE_RUNS)
    )