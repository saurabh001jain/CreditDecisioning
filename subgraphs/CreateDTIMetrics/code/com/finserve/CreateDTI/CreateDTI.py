from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def CreateDTI(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    df_Cleanup_1_1 = Cleanup_1_1(spark, in2)
    df_HidePII_1_1 = HidePII_1_1(spark, in1)
    df_ByCustomerID_1_1 = ByCustomerID_1_1(spark, in0, df_HidePII_1_1)
    df_SplitByTrade_1_1 = SplitByTrade_1_1(spark, df_ByCustomerID_1_1)
    df_Reorder_1_1 = Reorder_1_1(spark, df_Cleanup_1_1)
    df_Union_1_1 = Union_1_1(spark, df_SplitByTrade_1_1, df_Reorder_1_1)
    df_SumDebts_1_1 = SumDebts_1_1(spark, df_Union_1_1)

    return df_SumDebts_1_1
