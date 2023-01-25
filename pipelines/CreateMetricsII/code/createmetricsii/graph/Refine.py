from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Refine(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("name").alias("Name"), 
        col("monthly_loan_amount").cast(LongType()).alias("MonthlyBNPLLoanAmount"), 
        col("balance").alias("Balance")
    )
