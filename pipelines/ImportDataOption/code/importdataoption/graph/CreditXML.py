from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def CreditXML(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("xml")\
        .option("rowTag", "TRADES")\
        .option("mode", "PERMISSIVE")\
        .schema(StructType([]))\
        .load("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/credit/credit_report.xml")
