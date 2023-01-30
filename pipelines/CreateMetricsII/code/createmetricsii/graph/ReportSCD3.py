from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ReportSCD3(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3"):
        matched_expr = {}
        matched_expr["previousFicoScore"] = col("Target.FicoScore")
        matched_expr["FicoScore"] = col("Source.FicoScore")
        DeltaTable\
            .forPath(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3")\
            .alias("target")\
            .merge(in0.alias("source"), (col("Source.Name") == col("Target.Name")))\
            .whenMatchedUpdate(set = matched_expr)\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("overwriteSchema", True)\
            .mode("overwrite")\
            .save("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3")
