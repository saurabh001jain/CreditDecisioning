from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def Fico_Mod_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    #from pyspark.sql.functions import max, floor, rand, date_add, split, format_string, element_at
    #import _root.pyspark.sql.utils.AnalysisException
    from pyspark.sql.utils import AnalysisException

    try:
        old_df = spark.read.format("delta").load("dbfs:/FileStore/data/FICO_table_history")
        # Get the previous max date by ID
        # Add some random wiggle to the FICO scores
        # Select the same columns again to maintain order
        ws = Window.partitionBy("CUSTOMER_ID").orderBy(col("FICO_valid_until_date").desc())
        new_src = old_df.withColumn("row_num", row_number().over(ws)).where("row_num == 1").drop("row_num")
        w = new_src\
                .withColumn("date_FICORange_obtained", date_format(date_add("date_FICORange_obtained", 1), "dd-MM-yy"))\
                .withColumn("variation", floor((rand() - 0.5) * 50).cast("integer"))\
                .withColumn("ficosplit", split(col("FICORange"), "-"))\
                .withColumn(
                  "FICORange",
                  format_string(
                    "%d-%d",
                    (element_at(col("ficosplit"), 1) + col("variation")).cast("int"),
                    (element_at(col("ficosplit"), 2) + col("variation")).cast("int")
                  )
                )\
                .drop("ficosplit")\
                .drop("variation")\
                .select(col("CUSTOMER_ID").alias("ID"), "FICORange", "date_FICORange_obtained")
        out0 = w
    except AnalysisException as e:
        out0 = in0

    return out0
