from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Monthly(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Name"), 
        (col("ReportedIncome") / lit(12)).cast(LongType()).alias("Income"), 
        (col("TradLoanAmount") + col("BNPLLoanAmount")).cast(LongType()).alias("LoanTotal"), 
        col("FicoScore"), 
        col("FicoValidFrom"), 
        col("FicoValidTo"), 
        lit(True).alias("minFlag"), 
        lit(True).alias("maxFlag"), 
        lit("None").alias("previousFicoScore")
    )
