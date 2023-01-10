from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def JSON_BNPL(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
          lit("0.05")
        )
    )

    return requestDF.withColumn(
        "content_parsed",
        from_json(col("api_output.content"), schema_of_json(requestDF.select("api_output.content").take(1)[0][0]))
    )
