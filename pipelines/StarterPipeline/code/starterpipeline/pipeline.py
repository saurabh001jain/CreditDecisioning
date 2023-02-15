from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from starterpipeline.config.ConfigStore import *
from starterpipeline.udfs.UDFs import *
from prophecy.utils import *
from starterpipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Source_0 = Source_0(spark)
    df_RestAPIEnrich_1 = RestAPIEnrich_1(spark)
    Target_1(spark)
    df_Source_1 = Source_1(spark)
    df_SetOperation_1 = SetOperation_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/StarterPipeline")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/StarterPipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
