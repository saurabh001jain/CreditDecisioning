from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def PullData(spark: SparkSession):
    dbutils.fs.rm(
        '/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO${Year}.xml',
        recurse = True
    )
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD1', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD2', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportSCD3', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ReportPrevScoreSCD3', recurse = True)
    dbutils.fs.mkdirs('/Prophecy/finserv/ingest/FICO')
    dbutils.fs.mkdirs('/Prophecy/finserv/ingest/customer')
    dbutils.fs.mkdirs('/Prophecy/finserv/ingest/BNPL')
    import os
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2018.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2019 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2019.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2020 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2020.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/customer.csv -O /dbfs/Prophecy/finserv/ingest/customer/customer.csv'
    )
    os.system(
        'wget https://raw.githubusercontent.com/databricks/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/bnpl.json -O /dbfs/Prophecy/finserv/ingest/BNPL/bnpl.json'
    )

    return 
