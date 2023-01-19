from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def FileOperation_2_1(spark: SparkSession):
    if not 'SColumnExpression' in locals():
        from pyspark.dbutils import DBUtils
        DBUtils(spark).fs.cp(
            "file:/databricks/driver/bnpl.json",
            "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/bnpl/bnpl.json",
            recurse = True
        )
