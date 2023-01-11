from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *
from prophecy.utils import *
from createmetrics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_FICO_table_history = FICO_table_history(spark)
    df_Filter_1 = Filter_1(spark, df_FICO_table_history)
    df_Bureau_Source = Bureau_Source(spark)
    df_Reported_Income = Reported_Income(spark)
    df_BNPL_LexisNexis = BNPL_LexisNexis(spark)
    df_CreateDTI = CreateDTI(spark, df_Reported_Income, df_Bureau_Source, df_BNPL_LexisNexis)
    df_Source_1 = Source_1(spark)
    df_FICOScores = FICOScores(spark)
    df_ByCustomer = ByCustomer(spark, df_FICOScores, df_Reported_Income)
    df_ForSCD2 = ForSCD2(spark, df_ByCustomer)
    ReportMetrics_1(spark, df_CreateDTI)
    FICO_table(spark, df_ForSCD2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/CreateMetrics")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/CreateMetrics")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
