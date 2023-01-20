from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def TestAuditPrepGraph_2(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    df_Fico_Mod_1 = Fico_Mod_1(spark, in0)
    df_ByCustomer_1 = ByCustomer_1(spark, df_Fico_Mod_1, in1)
    df_ForSCD2_1 = ForSCD2_1(spark, df_ByCustomer_1)

    return df_ForSCD2_1
