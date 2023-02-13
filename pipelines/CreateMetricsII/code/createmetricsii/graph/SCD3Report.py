from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def SCD3Report(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/SCD3/ReportSCD3"):
        matched_expr = {}
        matched_expr["previousFicoScore"] = col("target.FicoScore")
        matched_expr["FicoScore"] = col("source.FicoScore")
        DeltaTable\
            .forPath(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/SCD3/ReportSCD3")\
            .alias("target")\
            .merge(in0.alias("source"), (col("source.Name") == col("target.Name")))\
            .whenMatchedUpdate(
              condition = (col("source.FicoValidFrom").cast(DateType()) > col("target.FicoValidFrom").cast(DateType())),
              set = matched_expr
            )\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("overwriteSchema", True)\
            .mode("overwrite")\
            .save("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/SCD3/ReportSCD3")
