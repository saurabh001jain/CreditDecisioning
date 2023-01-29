from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def Ingest(spark: SparkSession):
    import os
    os.system("mkdir -p /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO")
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO${Year} -O /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/FICO/TransUnionFICO${Year}.xml'
    )

    return 
