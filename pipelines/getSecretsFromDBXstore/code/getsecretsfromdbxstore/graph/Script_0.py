from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from getsecretsfromdbxstore.config.ConfigStore import *
from getsecretsfromdbxstore.udfs.UDFs import *

def Script_0(spark: SparkSession) -> DataFrame:
    out0 = dbutils.secrets.get(scope = "anyascopeUC", key = "AESkey")

    return out0
