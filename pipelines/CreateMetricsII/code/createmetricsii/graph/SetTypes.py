from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def SetTypes(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Name"), 
        (col("ReportedIncome") / lit(12)).cast(LongType()).alias("Income"), 
        (col("TradLoanAmount") + col("BNPLLoanAmount")).cast(LongType()).alias("LoanTotal"), 
        col("FicoScore"), 
        col("FicoValidFrom"), 
        col("FicoValidTo"), 
        lit(True).alias("minFlag"), 
        lit(True).alias("maxFlag"), 
        lit("").cast(LongType()).alias("previousFicoScore"), 
        (
          lit(100)
          * (
            (
              col("TradLoanAmount")
              + col("BNPLLoanAmount")
            )
            / (col("ReportedIncome") / lit(12)).cast(LongType())
          )\
            .cast(
            DecimalType(13, 4)
          )
        )\
          .alias(
          "DTIpercentage"
        )
    )
