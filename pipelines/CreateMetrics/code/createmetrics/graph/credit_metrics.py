from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def credit_metrics(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder
    in0.write.format("delta").mode("overwrite").save("dbfs:/Prophecy/sparklearner123@gmail.com/credit_metrics")
