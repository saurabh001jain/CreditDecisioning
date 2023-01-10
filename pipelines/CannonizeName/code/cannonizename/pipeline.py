from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cannonizename.config.ConfigStore import *
from cannonizename.udfs.UDFs import *
from prophecy.utils import *
from cannonizename.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Script_1 = Script_1(spark)
    df_RestAPIEnrich_1 = RestAPIEnrich_1(spark, df_Script_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/CannonizeName")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/CannonizeName")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
