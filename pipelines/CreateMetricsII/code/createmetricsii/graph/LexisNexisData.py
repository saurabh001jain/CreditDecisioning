from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def LexisNexisData(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("error").saveAsTable(f"lineage_data.default.LexisNexisData")
