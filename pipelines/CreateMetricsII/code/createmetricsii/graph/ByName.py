from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ByName(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Name") == col("in1.Name")), "inner")\
        .select(col("in0.ReportedIncome").alias("ReportedIncome"), col("in0.TransUnionReportedLoanMonthly").alias("TransUnionReportedLoanMonthly"), col("in1.Name").alias("Name"), col("in1.MonthlyBNPLLoanAmount").alias("MonthlyBNPLLoanAmount"), col("in1.Balance").alias("Balance"))
