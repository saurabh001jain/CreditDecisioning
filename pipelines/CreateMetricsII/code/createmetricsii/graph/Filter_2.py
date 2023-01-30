from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Filter_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
