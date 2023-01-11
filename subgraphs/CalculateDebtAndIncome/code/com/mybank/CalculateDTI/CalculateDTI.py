from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def CalculateDTI(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    df_Cleanup = Cleanup(spark, in2)
    df_Reorder = Reorder(spark, df_Cleanup)
    df_HidePII = HidePII(spark, in1)
    df_ByCustomerID = ByCustomerID(spark, in0, df_HidePII)
    df_SplitByTrade = SplitByTrade(spark, df_ByCustomerID)
    df_Reformat_1 = Reformat_1(spark, df_SplitByTrade)
    df_Union = Union(spark, df_Reformat_1, df_Reorder)
    df_SumDebts = SumDebts(spark, df_Union)

    return df_SumDebts
