from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ColumnDefinition(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("content_parsed.balance").alias("balance"), 
        col("content_parsed.lender_name").alias("lender_name"), 
        col("content_parsed.loan_id").alias("loan_id"), 
        col("content_parsed.monthly_loan_amount").alias("monthly_loan_amount"), 
        col("content_parsed.name").alias("name"), 
        col("content_parsed.past_due").alias("past_due"), 
        col("content_parsed.processor").alias("processor")
    )
