from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def DefineConfig(spark: SparkSession) -> DataFrame:
    spark.sql("set spark.databricks.userInfoFunctions.enabled = true").show()
    out0 = spark.sql("select current_user() as user_email").collect()
    Config.user_email = out0[0]['user_email']
    out0 = spark.range(0).select(lit(Config.user_email).alias('user_email'))

    return out0
