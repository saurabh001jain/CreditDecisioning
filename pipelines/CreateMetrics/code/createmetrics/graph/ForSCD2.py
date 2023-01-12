from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def ForSCD2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("CUSTOMER_NAME"), 
        col("FICORange"), 
        date_add(to_date(col("date_FICORange_obtained"), "dd-MM-yy"), 0).alias("date_FICORange_obtained"), 
        date_add(to_date(col("date_FICORange_obtained"), "dd-MM-yy"), 120).alias("FICO_valid_until_date"), 
        lit(True).alias("minFlag"), 
        lit(True).alias("maxFlag")
    )
