from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.withColumn("BNPL_data", explode_outer("BNPL_data")).columns
    selectCols = [col("balance") if "balance" in flt_col else col("BNPL_data.balance").alias("balance"),                   col("lender_name") if "lender_name" in flt_col else col("BNPL_data.lender_name").alias("lender_name"),                   col("loan_id") if "loan_id" in flt_col else col("BNPL_data.loan_id").alias("loan_id"),                   col("monthly_loan_amount") if "monthly_loan_amount" in flt_col else col("BNPL_data.monthly_loan_amount")\
                    .alias("monthly_loan_amount"),                   col("name") if "name" in flt_col else col("BNPL_data.name").alias("name"),                   col("past_due") if "past_due" in flt_col else col("BNPL_data.past_due").alias("past_due"),                   col("data-processor") if "data-processor" in flt_col else col("BNPL_data.processor").alias("data-processor"),                   col("data-term") if "data-term" in flt_col else col("BNPL_data.term").alias("data-term")]

    return in0.withColumn("BNPL_data", explode_outer("BNPL_data")).select(*selectCols)
