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
    df_GenericInput = GenericInput(spark)
    df_JSON_BNPL = JSON_BNPL(spark, df_GenericInput)
    df_SchemaTransform_5 = SchemaTransform_5(spark, df_JSON_BNPL)
    df_RestAPIEnrich_1 = RestAPIEnrich_1(spark, df_GenericInput)
    df_Reformat_3 = Reformat_3(spark, df_RestAPIEnrich_1)
    df_Reported_Income = Reported_Income(spark)
    df_Join_1 = Join_1(spark, df_Reported_Income, df_SchemaTransform_1)
    df_Flatten = Flatten(spark, df_Join_1)
    df_BNPL_LexisNexis = BNPL_LexisNexis(spark)
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_BNPL_LexisNexis)
    df_Reformat_1 = Reformat_1(spark, df_SchemaTransform_2)
    df_SetOperation_1 = SetOperation_1(spark, df_Flatten, df_Reformat_1)
    df_Aggregate_1 = Aggregate_1(spark, df_SetOperation_1)
    credit_metrics(spark, df_Aggregate_1)
    df_scores = scores(spark)
    df_Join_2 = Join_2(spark, df_Reported_Income, df_scores)
    df_Reformat_2 = Reformat_2(spark, df_Join_2)
    credit_score_table(spark, df_Reformat_2)
    df_credit_score_table_1 = credit_score_table_1(spark)
    df_SCD2 = SCD2(spark)
    df_Filter_1 = Filter_1(spark, df_credit_score_table_1)
    df_Script_1 = Script_1(spark, df_Reformat_3)
    df_Reformat_4 = Reformat_4(spark, df_Script_1)
    df_XML_CreditReport = XML_CreditReport(spark, df_GenericInput)
    df_SchemaTransform_4 = SchemaTransform_4(spark, df_XML_CreditReport)
    df_CSV_ReportedIncome = CSV_ReportedIncome(spark, df_GenericInput)
    df_Filter_2 = Filter_2(spark, df_Filter_1)
    df_Ingestion = Ingestion(spark)
    df_Filter_3 = Filter_3(spark, df_Reformat_4)
    df_SchemaTransform_3 = SchemaTransform_3(spark, df_CSV_ReportedIncome)

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
