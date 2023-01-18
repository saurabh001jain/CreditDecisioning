from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *
from prophecy.utils import *
from createmetrics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Bureau_Source = Bureau_Source(spark)
    df_BNPL_LexisNexis_v1 = BNPL_LexisNexis_v1(spark)
    df_DropCols = DropCols(spark, df_BNPL_LexisNexis_v1)
    df_ReorderCols = ReorderCols(spark, df_DropCols)
    df_Reported_Income = Reported_Income(spark)
    df_FICO_table_history = FICO_table_history(spark)
    df_OrderBy_1 = OrderBy_1(spark, df_FICO_table_history)
    df_Filter_1 = Filter_1(spark, df_OrderBy_1)
    df_ReportedIncome = ReportedIncome(spark)
    df_ByCustomerID_1_1_1 = ByCustomerID_1_1_1(spark, df_ReportedIncome, df_Bureau_Source)
    df_SplitByTrade_1_1_1 = SplitByTrade_1_1_1(spark, df_ByCustomerID_1_1_1)
    df_UnionDatasets = UnionDatasets(spark, df_SplitByTrade_1_1_1, df_ReorderCols)
    df_SumDebts = SumDebts(spark, df_UnionDatasets)
    df_FICOScores = FICOScores(spark)
    df_Fico_Mod = Fico_Mod(spark, df_FICOScores)
    df_ByCustomer = ByCustomer(spark, df_Fico_Mod, df_Reported_Income)
    df_ForSCD2 = ForSCD2(spark, df_ByCustomer)
    ReportMetrics_1(spark, df_SumDebts)
    FICO_table_hist(spark, df_ForSCD2)

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
