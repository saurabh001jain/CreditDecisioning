from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Gold_Credit_DTI_SCD2_CatalogTable(spark: SparkSession, in0: DataFrame):
    if spark.catalog._jcatalog.tableExists(f"{Config.database_name}.gold_credit_dti_SCD2"):
        from delta.tables import DeltaTable, DeltaMergeBuilder
        existingTable = DeltaTable.forName(spark, f"{Config.database_name}.gold_credit_dti_SCD2")
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
            .option("overwriteSchema", True)\
            .mode("overwrite")\
            .saveAsTable(f"{Config.database_name}.gold_credit_dti_SCD2")
