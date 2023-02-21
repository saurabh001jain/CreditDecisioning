from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def LexisNexis_json(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .option("multiLine", True)\
        .schema(
          StructType([
            StructField("BNPL_data", ArrayType(
            StructType([
              StructField("balance", LongType(), True), StructField("lender_name", StringType(), True), StructField("loan_id", LongType(), True), StructField("monthly_loan_amount", LongType(), True), StructField("name", StringType(), True), StructField("past_due", LongType(), True), StructField("processor", StringType(), True), StructField("term", StringType(), True)
          ]), 
            True
          ), True)
        ])
        )\
        .load("dbfs:/Prophecy/finserv/ingest/BNPL/bnpl_uc.json")
