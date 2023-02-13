from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def GenericInput_1(spark: SparkSession) -> DataFrame:
    out0 = spark.range(1)

    return out0
