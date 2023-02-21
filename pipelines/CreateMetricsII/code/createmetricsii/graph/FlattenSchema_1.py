from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("BNPL_data", explode_outer("BNPL_data"))\
        .select(col("BNPL_data.balance").alias("balance"), col("BNPL_data.lender_name").alias("lender_name"), col("BNPL_data.loan_id").alias("loan_id"), col("BNPL_data.monthly_loan_amount").alias("monthly_loan_amount"), col("BNPL_data.name").alias("name"), col("BNPL_data.past_due").alias("past_due"), col("BNPL_data.processor").alias("data-processor"), col("BNPL_data.term").alias("data-term"))
