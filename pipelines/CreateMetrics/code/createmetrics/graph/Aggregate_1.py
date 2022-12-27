from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("name"))

    return df1.agg(
        max((col("reported_income") / lit(12))).alias("income"), 
        sum(col("monthly_loan_amount")).alias("debt")
    )
