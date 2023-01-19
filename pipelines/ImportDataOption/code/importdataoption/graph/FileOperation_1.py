from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def FileOperation_1(spark: SparkSession):
    if not 'SColumnExpression' in locals():
        from pyspark.dbutils import DBUtils
        DBUtils(spark).fs.cp(
            "file:/databricks/driver/credit_report.xml",
            "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/credit/credit_report.xml",
            recurse = True
        )
