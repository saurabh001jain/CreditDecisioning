from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def Bureau_Source(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("xml")\
        .option("rowTag", "Subject")\
        .option("mode", "PERMISSIVE")\
        .schema(
          StructType([
            StructField("Address", StringType(), True), StructField("Name", StringType(), True), StructField("SSN", StringType(), True), StructField("Trades", StructType([
              StructField("Trade", ArrayType(
              StructType([
                StructField("AccountNumber", StringType(), True), StructField("Balance", LongType(), True), StructField("DateOpened", DateType(), True), StructField("PastDue", LongType(), True), StructField("Terms", StringType(), True)
            ]), 
              True
          ), True)
            ]), True)
        ])
        )\
        .load("dbfs:/FileStore/data/credit_report.xml")
