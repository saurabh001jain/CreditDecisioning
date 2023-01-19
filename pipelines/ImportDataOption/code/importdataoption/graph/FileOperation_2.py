from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def FileOperation_2(spark: SparkSession):
    if not 'SColumnExpression' in locals():
        from pyspark.dbutils import DBUtils
        DBUtils(spark).fs.cp(
            "file:/databricks/driver/customer.csv ",
            "dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/customer/customer.csv",
            recurse = True
        )
