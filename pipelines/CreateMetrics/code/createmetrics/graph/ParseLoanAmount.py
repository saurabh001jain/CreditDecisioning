from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def ParseLoanAmount(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("REPORTED_INCOME").cast(LongType()).alias("REPORTED_INCOME"), 
        col("Name"), 
        split(col("trades.trade")[0].getField("terms"), "M")[1].cast(LongType()).alias("monthly_loan_amount")
    )
