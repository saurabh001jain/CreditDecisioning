from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Income_OG(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("CUSTOMER_ID", StringType(), True), StructField("CUSTOMER_NAME", StringType(), True), StructField("REPORTED_INCOME", StringType(), True), StructField("DOB", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", "|")\
        .csv("dbfs:/Prophecy/finserv/ingest/customer/customer.csv")
