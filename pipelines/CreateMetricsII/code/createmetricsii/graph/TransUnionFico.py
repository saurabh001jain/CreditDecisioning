from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def TransUnionFico(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("xml")\
        .option("rowTag", "Subject")\
        .option("mode", "PERMISSIVE")\
        .schema(
          StructType([
            StructField("Address", StringType(), True), StructField("FICO", StructType([
              StructField("Score", LongType(), True), StructField("ValidFrom", DateType(), True), StructField("ValidTo", DateType(), True)
            ]), True), StructField("Name", StringType(), True), StructField("SSN", StringType(), True), StructField("Trades", StructType([
              StructField("Trade", ArrayType(
              StructType([
                StructField("AccountNumber", StringType(), True), StructField("Balance", LongType(), True), StructField("DateOpened", DateType(), True), StructField("PastDue", LongType(), True), StructField("Terms", StringType(), True)
            ]), 
              True
          ), True)
            ]), True)
        ])
        )\
        .load(f"dbfs:/Prophecy/finserv/ingest/FICO/TransUnionFICO{Config.Year}.xml")
