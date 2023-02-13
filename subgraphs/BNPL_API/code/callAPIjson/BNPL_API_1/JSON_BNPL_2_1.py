from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def JSON_BNPL_2_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.udfs import get_rest_api
    requestDF = in0.withColumn(
        "api_output",
        get_rest_api(
          to_json(
            struct(
              lit("GET").alias("method"), 
              lit(
                  "https://raw.githubusercontent.com/databricks/terraform-databricks-lakehouse-blueprints/prophecy_quickstart/industry/fsi/data/bnpl.json"
                )\
                .alias("url")
            )
          ),
          lit("")
        )
    )

    return requestDF
