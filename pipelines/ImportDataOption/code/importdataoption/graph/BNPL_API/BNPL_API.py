from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def BNPL_API(spark: SparkSession) -> DataFrame:
    df_GenericInput_1 = GenericInput_1(spark)
    df_JSON_BNPL_2_1 = JSON_BNPL_2_1(spark, df_GenericInput_1)
    df_Explode = Explode(spark, df_JSON_BNPL_2_1)
    df_ParseJson = ParseJson(spark, df_Explode)
    df_ColumnDefinition = ColumnDefinition(spark, df_ParseJson)
    df_Display = Display(spark, df_ColumnDefinition)

    return df_Display
