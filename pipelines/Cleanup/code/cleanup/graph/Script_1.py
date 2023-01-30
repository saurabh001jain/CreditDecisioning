from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cleanup.config.ConfigStore import *
from cleanup.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    spark.sql("TRUNCATE TABLE delta.`dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportWithHistory`")
    spark.sql(
        "TRUNCATE TABLE delta.`dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2018.xml`"
    )
    spark.sql(
        "TRUNCATE TABLE delta.`dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2019.xml`"
    )
    spark.sql(
        "TRUNCATE TABLE delta.`dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2020.xml`"
    )
    spark.sql("TRUNCATE TABLE delta.`dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportWithHistory`")
    spark.sql("TRUNCATE TABLE delta.`dbfs:/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3`")
    out0 = in0

    return out0
