from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def EncryptPII(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        expr("aes_encrypt(SSN, '1234567890123456')").alias("SSN"), 
        col("Address"), 
        col("FICO"), 
        col("FICO.Score").alias("Score"), 
        col("FICO.ValidFrom").alias("ValidFrom"), 
        col("FICO.ValidTo").alias("ValidTo"), 
        col("Name"), 
        col("Trades"), 
        col("Trades.Trade").alias("Trade"), 
        col("Trades.Trade.AccountNumber").alias("AccountNumber"), 
        col("Trades.Trade.Balance").alias("Balance"), 
        col("Trades.Trade.DateOpened").alias("DateOpened"), 
        col("Trades.Trade.PastDue").alias("PastDue"), 
        col("Trades.Trade.Terms").alias("Terms")
    )
