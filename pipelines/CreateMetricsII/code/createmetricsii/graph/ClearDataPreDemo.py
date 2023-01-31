from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def ClearDataPreDemo(spark: SparkSession):
    dbutils.fs.rm(
        '/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2018.xml',
        recurse = True
    )
    dbutils.fs.rm(
        '/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2019.xml',
        recurse = True
    )
    dbutils.fs.rm(
        '/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2020.xml',
        recurse = True
    )
    dbutils.fs.rm(
        '/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2021.xml',
        recurse = True
    )
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportWithHistory', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD1', recurse = True)

    return 
