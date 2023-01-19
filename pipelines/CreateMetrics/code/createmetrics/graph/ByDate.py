from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def ByDate(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("CUSTOMER_ID").asc(), col("date_FICORange_obtained").desc())
