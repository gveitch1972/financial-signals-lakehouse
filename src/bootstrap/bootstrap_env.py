from pyspark.shell import spark


spark.sql("CREATE CATALOG IF NOT EXISTS fin_signals_dev")
spark.sql("CREATE SCHEMA IF NOT EXISTS fin_signals_dev.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS fin_signals_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS fin_signals_dev.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS fin_signals_dev.audit")