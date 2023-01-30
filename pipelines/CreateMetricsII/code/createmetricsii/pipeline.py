from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *
from prophecy.utils import *
from createmetricsii.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DefineConfig = DefineConfig(spark)
    UseConfig(spark)
    Ingest(spark)
    df_LexisNexis = LexisNexis(spark)
    df_Income = Income(spark)
    df_TransUnionFico_xml = TransUnionFico_xml(spark)
    df_ByCustomerID = ByCustomerID(spark, df_Income, df_TransUnionFico_xml)
    df_testConfig = testConfig(spark)
    df_Filter_2 = Filter_2(spark, df_DefineConfig)
    df_Refine = Refine(spark, df_LexisNexis)
    df_ParseLoanAmount = ParseLoanAmount(spark, df_ByCustomerID)
    df_ByName = ByName(spark, df_ParseLoanAmount, df_Refine)
    df_Monthly = Monthly(spark, df_ByName)
    ReportWithHistory(spark, df_Monthly)
    df_Reformat_1 = Reformat_1(spark, df_LexisNexis)
    Profile(spark, df_Monthly)
    df_ReportSCD3_1 = ReportSCD3_1(spark)
    df_OrderBy_1 = OrderBy_1(spark, df_ReportSCD3_1)
    df_Filter_3 = Filter_3(spark, df_Reformat_1)
    ReportSCD3(spark, df_Monthly)
    Report(spark)
    df_ReportWithHistory_1 = ReportWithHistory_1(spark)
    df_OrderBy = OrderBy(spark, df_ReportWithHistory_1)
    df_Filter_1 = Filter_1(spark, df_OrderBy)
    ReportLatest(spark, df_Filter_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/CreateMetricsII")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/CreateMetricsII")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
