from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cleanup.config.ConfigStore import *
from cleanup.udfs.UDFs import *

def ReportWithHistory(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("delta")\
        .load("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportWithHistory")
