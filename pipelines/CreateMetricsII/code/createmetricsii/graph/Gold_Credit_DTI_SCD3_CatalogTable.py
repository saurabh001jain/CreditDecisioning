from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Gold_Credit_DTI_SCD3_CatalogTable(spark: SparkSession, in0: DataFrame):
    if spark.catalog._jcatalog.tableExists("`FSIdemos.gold`.`Credit_DTI_SCD3`"):
        from delta.tables import DeltaTable, DeltaMergeBuilder
        DeltaTable\
            .forName(spark, "`FSIdemos.gold`.`Credit_DTI_SCD3`")\
            .alias("target")\
            .merge(in0.alias("source"), (col("target.Name") == col("source.Name")))\
            .whenMatchedUpdate(
              condition = (col("source.FicoValidFrom").cast(DateType()) > col("target.FicoValidFrom").cast(DateType())),
              set = {"previousFicoScore" : col("target.FicoScore"), "FicoScore" : col("source.FicoScore")}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("overwriteSchema", True)\
            .mode("overwrite")\
            .saveAsTable("`FSIdemos.gold`.`Credit_DTI_SCD3`")
