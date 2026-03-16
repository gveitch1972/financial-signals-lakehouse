from pyspark.sql import SparkSession

from src.common.audit import log_pipeline_run
from src.tests.validate_pipeline import run_validations


spark = SparkSession.builder.getOrCreate()
try:
    summary = run_validations(spark)
    print(summary)
except Exception as error:
    log_pipeline_run(
        spark,
        pipeline_name="validation_queries",
        status="FAILED",
        row_count=0,
        message=str(error),
    )
    raise
