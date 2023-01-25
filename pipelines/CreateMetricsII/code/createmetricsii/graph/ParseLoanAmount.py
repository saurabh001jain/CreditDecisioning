from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ParseLoanAmount(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("REPORTED_INCOME").cast(LongType()).alias("ReportedIncome"), 
        col("Name"), 
        split(col("trades.trade")[0].getField("terms"), "M")[1].cast(LongType()).alias("TransUnionReportedLoanMonthly")
    )
