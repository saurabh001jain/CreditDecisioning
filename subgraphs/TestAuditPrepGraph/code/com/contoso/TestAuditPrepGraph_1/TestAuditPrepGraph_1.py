from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def TestAuditPrepGraph_1(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    df_Fico_Mod = Fico_Mod(spark, in0)
    df_ByCustomer = ByCustomer(spark, df_Fico_Mod, in1)
    df_ForSCD2 = ForSCD2(spark, df_ByCustomer)

    return df_ForSCD2
