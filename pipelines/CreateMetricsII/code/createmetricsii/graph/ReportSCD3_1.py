from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ReportSCD3_1(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3")
