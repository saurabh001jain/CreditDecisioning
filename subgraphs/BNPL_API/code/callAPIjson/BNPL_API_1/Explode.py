from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Explode(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(explode(split(col("api_output.content"), "\n")).alias("content"))
