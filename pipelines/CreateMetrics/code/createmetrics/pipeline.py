from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *
from prophecy.utils import *
from createmetrics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Income = Income(spark)
    df_LexisNexis = LexisNexis(spark)
    df_DropCols = DropCols(spark, df_LexisNexis)
    df_ReorderCols = ReorderCols(spark, df_DropCols)
    df_CustIncome = CustIncome(spark)
    df_TransUnion = TransUnion(spark)
    df_ByCustomerID = ByCustomerID(spark, df_Income, df_TransUnion)
    df_ParseAmount = ParseAmount(spark, df_ByCustomerID)
    df_UnionData = UnionData(spark, df_ParseAmount, df_ReorderCols)
    df_DebtsMonthly = DebtsMonthly(spark, df_UnionData)
    ReportDTI(spark, df_DebtsMonthly)
    df_FICOScores = FICOScores(spark)
    df_AuditLogic = AuditLogic(spark, df_FICOScores, df_CustIncome)
    FICO_table_history_1(spark, df_AuditLogic)
    df_FICO_table_history = FICO_table_history(spark)
    df_ByDate = ByDate(spark, df_FICO_table_history)
    df_Display = Display(spark, df_ByDate)

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
