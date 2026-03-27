import os
import sys

from pyspark.sql import SparkSession

cwd = os.getcwd()
root = cwd

while root != "/" and not os.path.isdir(os.path.join(root, "src")):
    root = os.path.dirname(root)

print("Running daily_market_snapshot")
print("MARKER: gold-runner-v1")
print("CWD:", cwd)
print("DISCOVERED_ROOT:", root)
print("SRC_EXISTS:", os.path.isdir(os.path.join(root, "src")))

if root == "/" or not os.path.isdir(os.path.join(root, "src")):
    raise RuntimeError(f"Could not find project root containing src/. Started from: {cwd}")

if root not in sys.path:
    sys.path.insert(0, root)

print("sys.path[0]:", sys.path[0])

from src.common.audit import log_pipeline_run
from src.gold.build_cross_signal_summary import build_cross_signal_summary
from src.gold.build_daily_market_snapshot import build_daily_market_snapshot
from src.gold.build_fx_trend_signals import build_fx_trend_signals
from src.gold.build_macro_indicator_trends import build_macro_indicator_trends
from src.gold.build_top_movers_why import build_top_movers_why


def safe_log_pipeline_run(spark, pipeline_name, status, row_count, message=None):
    try:
        log_pipeline_run(
            spark,
            pipeline_name=pipeline_name,
            status=status,
            row_count=row_count,
            message=message,
        )
    except Exception as log_error:
        print(f"Audit logging skipped due to error: {log_error}")


def run_and_audit(spark: SparkSession, pipeline_name: str, fn):
    try:
        result = fn(spark)
        row_count = result.count()
        safe_log_pipeline_run(
            spark,
            pipeline_name=pipeline_name,
            status="SUCCESS",
            row_count=row_count,
            message=f"{pipeline_name} completed",
        )
        return row_count
    except Exception as error:
        safe_log_pipeline_run(
            spark,
            pipeline_name=pipeline_name,
            status="FAILED",
            row_count=0,
            message=str(error),
        )
        raise


if __name__ == "__main__":
    print("Running gold layer pipelines")
    spark = SparkSession.builder.getOrCreate()

    try:
        total_rows = 0
        total_rows += run_and_audit(spark, "gold_daily_market_snapshot", build_daily_market_snapshot)
        total_rows += run_and_audit(spark, "gold_fx_trend_signals", build_fx_trend_signals)
        total_rows += run_and_audit(spark, "gold_macro_indicator_trends", build_macro_indicator_trends)
        total_rows += run_and_audit(spark, "gold_cross_signal_summary", build_cross_signal_summary)
        total_rows += run_and_audit(spark, "gold_top_movers_why", build_top_movers_why)

        safe_log_pipeline_run(
            spark,
            pipeline_name="gold_analytics",
            status="SUCCESS",
            row_count=total_rows,
            message="gold analytics orchestration completed",
        )
    except Exception as error:
        safe_log_pipeline_run(
            spark,
            pipeline_name="gold_analytics",
            status="FAILED",
            row_count=0,
            message=str(error),
        )
        raise
