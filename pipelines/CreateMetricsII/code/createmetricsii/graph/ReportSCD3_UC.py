from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ReportSCD3_UC(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, f"dbfs:/Prophecy/{Config.user_email}/finserv/prophecy/gold_credit_dti_SCD3_UC"):
        matched_expr = {}
        matched_expr["previousFicoScore"] = col("target.FicoScore")
        matched_expr["FicoScore"] = col("source.FicoScore")
        DeltaTable\
            .forPath(spark, f"dbfs:/Prophecy/{Config.user_email}/finserv/prophecy/gold_credit_dti_SCD3_UC")\
            .alias("target")\
            .merge(in0.alias("source"), (col("target.Name") == col("source.Name")))\
            .whenMatchedUpdate(
              condition = (col("source.FicoValidFrom").cast(DateType()) > col("target.FicoValidFrom").cast(DateType())),
              set = matched_expr
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("overwriteSchema", False)\
            .mode("overwrite")\
            .save(f"dbfs:/Prophecy/{Config.user_email}/finserv/prophecy/gold_credit_dti_SCD3_UC")
