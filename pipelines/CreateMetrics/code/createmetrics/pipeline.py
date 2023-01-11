from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *
from prophecy.utils import *
from createmetrics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Bureau_Source = Bureau_Source(spark)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Bureau_Source)
    df_Reported_Income = Reported_Income(spark)
    df_Join_1 = Join_1(spark, df_Reported_Income, df_SchemaTransform_1)
    df_Flatten = Flatten(spark, df_Join_1)
    df_BNPL_LexisNexis = BNPL_LexisNexis(spark)
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_BNPL_LexisNexis)
    df_Reformat_1 = Reformat_1(spark, df_SchemaTransform_2)
    df_SetOperation_1 = SetOperation_1(spark, df_Flatten, df_Reformat_1)
    df_Aggregate_1 = Aggregate_1(spark, df_SetOperation_1)
    credit_metrics(spark, df_Aggregate_1)
    df_loan_data = loan_data(spark)
    df_Join_3 = Join_3(spark, df_loan_data, df_Reported_Income)
    df_Reformat_4 = Reformat_4(spark, df_Join_3)
    SCD2_report(spark, df_Reformat_4)
    df_FICO_historical_table = FICO_historical_table(spark)
    df_Filter_3 = Filter_3(spark, df_FICO_historical_table)

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
