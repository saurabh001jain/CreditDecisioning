from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def Flatten(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("monthly_loan_amount", split(col("trades.trade")[0].getField("terms"), "M")[1].cast(LongType()))\
        .drop("CUSTOMER_ID")\
        .drop("DOB")\
        .drop("SSN")\
        .drop("Trades")\
        .drop("CUSTOMER_NAME")\
        .drop("Address")
