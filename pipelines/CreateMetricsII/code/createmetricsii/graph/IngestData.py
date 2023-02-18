from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def IngestData(spark: SparkSession):
    # Config vars can easily be passed to out-of-the-box gems
    # Config vars passing to script gems seems more complicated
    # So i am hard coding the values for this script gem
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
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/gold_credit_dti_SCD1', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/gold_credit_dti_SCD2', recurse = True)
    dbutils.fs.rm('/Prophecy/sparklearner123@gmail.com/finserv/prophecy/gold_credit_dti_SCD3_UC', recurse = True)
    import os
    os.system("mkdir -p /dbfs/Prophecy/")
    os.system("mkdir -p /dbfs/Prophecy/finserv")
    os.system("mkdir -p /dbfs/Prophecy/finserv/ingest")
    os.system("mkdir -p /dbfs/Prophecy/finserv/ingest/FICO")
    os.system("mkdir -p /dbfs/Prophecy/finserv/ingest/BNPL")
    os.system("mkdir -p /dbfs/Prophecy/finserv/ingest/customer")
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2018.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2019.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2020.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/finserv/ingest/FICO/TransUnionFICO2021.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/customer.csv -O /dbfs/Prophecy/finserv/ingest/customer/customer.csv'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/bnpl.json -O /dbfs/Prophecy/finserv/ingest/BNPL/bnpl.json'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/bnpl_uc.json -O /dbfs/Prophecy/finserv/ingest/BNPL/bnpl_uc.json'
    )

    return 
