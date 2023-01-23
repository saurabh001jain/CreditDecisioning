from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def Refine(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(None).cast(StringType()).alias("REPORTED_INCOME"), 
        col("name").alias("Name"), 
        col("monthly_loan_amount").cast(LongType()).alias("monthly_loan_amount")
    )
