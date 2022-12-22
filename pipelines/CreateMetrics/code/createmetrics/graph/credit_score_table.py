from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def credit_score_table(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/credit_score_table"):
        existingTable = DeltaTable.forPath(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/credit_score_table")
        updatesDF = in0.withColumn("minFlag", lit("true")).withColumn("maxFlag", lit("true"))
        existingDF = existingTable.toDF()
        updateColumns = updatesDF.columns
        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["CUSTOMER_NAME", "Range_valid_boolean_case"])\
                              .where((existingDF["maxFlag"] == lit("true")) & (existingDF["Range"] != updatesDF["Range"]))\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("minFlag", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("CUSTOMER_NAME", "Range_valid_boolean_case")))
        existingTable\
            .alias("existingTable")\
            .merge(
              stagedUpdatesDF.alias("staged_updates"),
              concat(existingDF["CUSTOMER_NAME"], existingDF["Range_valid_boolean_case"]) == stagedUpdatesDF["mergeKey"]
            )\
            .whenMatchedUpdate(
              condition = (existingDF["maxFlag"] == lit("true")) & (existingDF["Range"] != stagedUpdatesDF["Range"]),
              set = {
"maxFlag" : "false", "Range_valid_until_date" : "staged_updates.date_of_FICO_score"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/Prophecy/sparklearner123@gmail.com/credit_score_table")
