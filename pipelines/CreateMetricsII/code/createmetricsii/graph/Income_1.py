from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Income_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"lineage_data.default.income")
