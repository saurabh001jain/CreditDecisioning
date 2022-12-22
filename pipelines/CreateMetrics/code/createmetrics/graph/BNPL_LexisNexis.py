from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def BNPL_LexisNexis(spark: SparkSession) -> DataFrame:
    return spark.read.format("json").load("dbfs:/FileStore/data/bnpl.json")
