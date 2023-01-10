from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def GenericInput(spark: SparkSession) -> DataFrame:
    out0 = spark.range(1)

    return out0
