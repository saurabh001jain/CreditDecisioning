from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetricsii.config.ConfigStore import *
from createmetricsii.udfs.UDFs import *

def UseConfig(spark: SparkSession):
    #goal: /dbfs/Prophecy/${user_email}/finserv/prophecy/ingest/FICO/TransUnionFICO${Year}.xml
    import os
    os.system("mkdir -p /dbfs/Prophecy/${user_email}/finserv/prophecy/ingest/FICO")
    os.system(
        'wget https://raw.githubusercontent.com/atbida/terraform-databricks-lakehouse-blueprints/main/industry/fsi/data/TransUnionFICO2018 -O /dbfs/Prophecy/${user_email}/finserv/prophecy/ingest/test.xml'
    )

    return 
