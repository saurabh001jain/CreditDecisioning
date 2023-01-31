from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ReportSCD2(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD2"):
        existingTable = DeltaTable.forPath(
            spark,
            "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD2"
        )
        updatesDF = in0.withColumn("minFlag", lit("true")).withColumn("maxFlag", lit("true"))
        existingDF = existingTable.toDF()
        updateColumns = updatesDF.columns
        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["Name"])\
                              .where(
                                (
                                  (existingDF["maxFlag"] == lit("true"))
                                  & (
                                    existingDF["FicoScore"]
                                    != updatesDF["FicoScore"]
                                  )
                                )
                              )\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("minFlag", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("Name")))
        existingTable\
            .alias("existingTable")\
            .merge(stagedUpdatesDF.alias("staged_updates"), concat(existingDF["Name"]) == stagedUpdatesDF["mergeKey"])\
            .whenMatchedUpdate(
              condition = (existingDF["maxFlag"] == lit("true")) & (existingDF["FicoScore"] != stagedUpdatesDF["FicoScore"]),
              set = {
"maxFlag" : "false", "FicoValidTo" : "staged_updates.FicoValidFrom"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .mode("overwrite")\
            .save("dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD2")
