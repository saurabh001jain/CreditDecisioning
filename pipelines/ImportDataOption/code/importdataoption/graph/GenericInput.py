from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def GenericInput(spark: SparkSession) -> DataFrame:
    out0 = spark.range(1)

    return out0
