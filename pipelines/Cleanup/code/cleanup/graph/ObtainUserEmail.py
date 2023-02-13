from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cleanup.config.ConfigStore import *
from cleanup.udfs.UDFs import *

def ObtainUserEmail(spark: SparkSession):
    spark.sql("set spark.databricks.userInfoFunctions.enabled = true").show()
    out0 = spark.sql("select current_user() as user_email").collect()
    Config.user_email = out0[0]['user_email']

    return 
