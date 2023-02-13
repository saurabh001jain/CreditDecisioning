from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_1(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    df_Refine_1 = Refine_1(spark, in1)
    df_ParseLoanAmount_1 = ParseLoanAmount_1(spark, in0)
    df_ByName_1 = ByName_1(spark, df_ParseLoanAmount_1, df_Refine_1)
    df_AddCols_1 = AddCols_1(spark, df_ByName_1)

    return df_AddCols_1
