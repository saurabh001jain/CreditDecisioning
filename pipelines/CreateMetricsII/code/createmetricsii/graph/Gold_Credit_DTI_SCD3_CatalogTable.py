from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Gold_Credit_DTI_SCD3_CatalogTable(spark: SparkSession, in0: DataFrame):
    if spark.catalog._jcatalog.tableExists(f"{Config.database_name}.gold_credit_dti_SCD3_UC"):
        from delta.tables import DeltaTable, DeltaMergeBuilder
        matched_expr = {}
        matched_expr["previousFicoScore"] = col("target.FicoScore")
        matched_expr["FicoScore"] = col("source.FicoScore")
        DeltaTable\
            .forName(spark, f"{Config.database_name}.gold_credit_dti_SCD3_UC")\
            .alias("target")\
            .merge(in0.alias("source"), (col("target.Name") == col("source.Name")))\
            .whenMatchedUpdate(
              condition = (col("source.FicoValidFrom").cast(DateType()) > col("target.FicoValidFrom").cast(DateType())),
              set = matched_expr
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("overwriteSchema", True)\
            .mode("overwrite")\
            .saveAsTable(f"{Config.database_name}.gold_credit_dti_SCD3_UC")
