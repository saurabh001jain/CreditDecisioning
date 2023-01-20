from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def FICO_table_history_1(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/FileStore/data/FICO_table_history"):
        existingTable = DeltaTable.forPath(spark, "dbfs:/FileStore/data/FICO_table_history")
        updatesDF = in0.withColumn("minFlag", lit("true")).withColumn("maxFlag", lit("true"))
        existingDF = existingTable.toDF()
        updateColumns = updatesDF.columns
        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["CUSTOMER_ID"])\
                              .where(
                                (
                                  (existingDF["maxFlag"] == lit("true"))
                                  & (
                                    existingDF["FICORange"]
                                    != updatesDF["FICORange"]
                                  )
                                )
                              )\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("minFlag", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("CUSTOMER_ID")))
        existingTable\
            .alias("existingTable")\
            .merge(stagedUpdatesDF.alias("staged_updates"), concat(existingDF["CUSTOMER_ID"]) == stagedUpdatesDF["mergeKey"])\
            .whenMatchedUpdate(
              condition = (existingDF["maxFlag"] == lit("true")) & (existingDF["FICORange"] != stagedUpdatesDF["FICORange"]),
              set = {
"maxFlag" : "false", "FICO_valid_until_date" : "staged_updates.date_FICORange_obtained"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("overwriteSchema", True)\
            .mode("overwrite")\
            .save("dbfs:/FileStore/data/FICO_table_history")
