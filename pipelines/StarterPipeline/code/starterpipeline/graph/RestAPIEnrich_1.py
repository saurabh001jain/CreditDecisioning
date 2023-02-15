from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from starterpipeline.config.ConfigStore import *
from starterpipeline.udfs.UDFs import *

def RestAPIEnrich_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.udfs import get_rest_api
    requestDF = in0.withColumn(
        "api_output",
        get_rest_api(
          to_json(struct(lit("GET").alias("method"), lit("https://www.boredapi.com/api/activity").alias("url"))),
          lit("")
        )
    )

    return requestDF
