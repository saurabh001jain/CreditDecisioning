from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def Customer(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("CUSTOMER_ID", StringType(), True), StructField("CUSTOMER_NAME", StringType(), True), StructField("REPORTED_INCOME", StringType(), True), StructField("DOB", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", "|")\
        .csv("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/customer/customer.csv")
