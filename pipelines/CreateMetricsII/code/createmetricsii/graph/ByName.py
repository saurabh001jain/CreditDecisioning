from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ByName(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Name") == col("in1.Name")), "inner")\
        .select(col("in1.Name").alias("Name"), col("in0.ReportedIncome").alias("ReportedIncome"), col("in0.TULoanAmount").alias("TradLoanAmount"), col("in1.MonthlyBNPLLoanAmount").alias("BNPLLoanAmount"), col("in1.Balance").alias("Balance"), col("in0.Score").alias("FicoScore"), col("in0.ValidFrom").alias("FicoValidFrom"), col("in0.ValidTo").alias("FicoValidTo"))
