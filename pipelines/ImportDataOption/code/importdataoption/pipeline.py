from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *
from prophecy.utils import *
from importdataoption.graph import *

def pipeline(spark: SparkSession) -> None:
    df_GenericInput = GenericInput(spark)
    df_JSON_BNPL = JSON_BNPL(spark, df_GenericInput)
    df_SchemaTransform_5 = SchemaTransform_5(spark, df_JSON_BNPL)
    df_JSON_BNPL_2 = JSON_BNPL_2(spark, df_GenericInput)
    df_Reformat_3 = Reformat_3(spark, df_JSON_BNPL_2)
    df_Script_1 = Script_1(spark, df_Reformat_3)
    df_Reformat_4 = Reformat_4(spark, df_Script_1)
    df_CSV_ReportedIncome = CSV_ReportedIncome(spark, df_GenericInput)
    df_SchemaTransform_3 = SchemaTransform_3(spark, df_CSV_ReportedIncome)
    df_XML_CreditReport = XML_CreditReport(spark, df_GenericInput)
    df_Reformat_5 = Reformat_5(spark, df_XML_CreditReport)
    df_Filter_3 = Filter_3(spark, df_Reformat_4)

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
