from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *
from prophecy.utils import *
from importdataoption.graph import *

def pipeline(spark: SparkSession) -> None:
    wgetCUSTcsv(spark)
    wgetCREDxml(spark)
    wgetBNPLjson(spark)
    df_CustomerCSV = CustomerCSV(spark)
    df_BNPL_API = BNPL_API(spark)
    df_BNPLjson = BNPLjson(spark)
    FileOperation_2_1(spark)
    df_Filter_1 = Filter_1(spark, df_BNPL_API)
    FileOperation_2(spark)
    FileOperation_1(spark)
    df_CreditXML = CreditXML(spark)

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
