from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def RestAPIEnrich_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.udfs import get_rest_api
    requestDF = in0.withColumn(
        "api_output",
        get_rest_api(
          to_json(
            struct(
              lit("GET").alias("method"), 
              lit(
                  "https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/a362f4ff309986d434e0c5931b1724a6fd5af3e9/industry/fsi/data/loan_data.csv"
                )\
                .alias("url")
            )
          ),
          lit("")
        )
    )

    return requestDF
