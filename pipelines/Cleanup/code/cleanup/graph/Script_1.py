from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cleanup.config.ConfigStore import *
from cleanup.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    spark.sql("TRUNCATE TABLE delta.`dbfs:/FileStore/data/FICO_table_history`")
    out0 = in0

    return out0
