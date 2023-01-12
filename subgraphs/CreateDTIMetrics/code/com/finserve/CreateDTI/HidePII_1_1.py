from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def HidePII_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("SSN", base64(col("SSN")))
