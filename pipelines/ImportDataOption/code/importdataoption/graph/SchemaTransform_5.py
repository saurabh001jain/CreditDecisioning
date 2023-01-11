from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def SchemaTransform_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("id", expr("split(content_parsed.loan_id, id)"))
