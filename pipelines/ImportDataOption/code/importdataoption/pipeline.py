from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *
from prophecy.utils import *
from importdataoption.graph import *

def pipeline(spark: SparkSession) -> None:
    IngestCSV(spark)
    IngestXML(spark)
    IngestJSON(spark)
    df_Customer = Customer(spark)
    df_Source_1 = Source_1(spark)
    df_LexisNexis = LexisNexis(spark)
    df_BNPL_API_1 = BNPL_API_1(spark)
    df_TransUnion = TransUnion(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/ImportDataOption")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/ImportDataOption")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
