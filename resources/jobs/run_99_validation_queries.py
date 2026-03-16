import os
import sys

from pyspark.sql import SparkSession

cwd = os.getcwd()
root = cwd

while root != "/" and not os.path.isdir(os.path.join(root, "src")):
    root = os.path.dirname(root)

if root == "/" or not os.path.isdir(os.path.join(root, "src")):
    raise RuntimeError(f"Could not find project root containing src/. Started from: {cwd}")

if root not in sys.path:
    sys.path.insert(0, root)

from src.common.audit import log_pipeline_run
from src.tests.validate_pipeline import run_validations


if __name__ == "__main__":
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
