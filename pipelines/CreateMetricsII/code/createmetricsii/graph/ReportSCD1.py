from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ReportSCD1(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .save(f"dbfs:/Prophecy/{Config.user_email}/finserv/prophecy/gold_credit_dti_SCD1")
