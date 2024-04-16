from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from createmetricsii.udfs.UDFs import *
from . import *
from .config import *

def Encryption(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Encrypt = Encrypt(spark, in0)
    subgraph_config.update(Config)

    return df_Encrypt
