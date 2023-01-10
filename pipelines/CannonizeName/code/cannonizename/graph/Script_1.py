from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cannonizename.config.ConfigStore import *
from cannonizename.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = spark.range(1)

    return out0
