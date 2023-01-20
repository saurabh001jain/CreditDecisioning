from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def IngestJSON(spark: SparkSession):
    import os
    os.system("mkdir -p /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/data")
    os.system(
        'wget https://raw.githubusercontent.com/databricks/terraform-databricks-lakehouse-blueprints/prophecy_quickstart/industry/fsi/data/bnpl.json -O /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/data/bnpl.json'
    )

    return 
