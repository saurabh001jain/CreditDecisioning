from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def Reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("CUSTOMER_NAME"), 
        col("FICORange").alias("Range"), 
        date_add(to_date(col("date_of_FICO_score"), "dd-MM-yy"), 121).alias("Range_valid_until_date"), 
        when((current_date().cast(DoubleType()) <= (lit(120) + col("date_of_FICO_score"))), lit("true"))\
          .otherwise(lit("false"))\
          .alias("Range_valid_boolean_case"), 
        date_add(to_date(col("date_of_FICO_score"), "dd-MM-yy"), 1).alias("date_of_FICO_score"), 
        lit(True).alias("minFlag"), 
        lit(True).alias("maxFlag")
    )
