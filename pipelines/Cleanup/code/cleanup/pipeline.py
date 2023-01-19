from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cleanup.config.ConfigStore import *
from cleanup.udfs.UDFs import *
from prophecy.utils import *
from cleanup.graph import *

def pipeline(spark: SparkSession) -> None:
    df_FICO_table_history_1 = FICO_table_history_1(spark)
    df_Script_1 = Script_1(spark, df_FICO_table_history_1)
    df_Filter_1 = Filter_1(spark, df_Script_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Cleanup")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/Cleanup")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
