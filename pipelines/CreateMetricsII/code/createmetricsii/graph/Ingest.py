from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Ingest(spark: SparkSession):
    import os
    os.system("mkdir -p /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO")
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2018.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2019 -O /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2019.xml'
    )
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2020 -O /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO2020.xml'
    )

    return 
