from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from importdataoption.config.ConfigStore import *
from importdataoption.udfs.UDFs import *

def IngestXML(spark: SparkSession):
    import os
    os.system("mkdir -p /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/creditreport")
    os.system(
        'wget https://raw.githubusercontent.com/databricks/terraform-databricks-lakehouse-blueprints/prophecy_quickstart/industry/fsi/data/credit_report.xml -O /dbfs/Prophecy/sparklearner123@gmail.com/finserv/prophecy/ingest/creditreport/credit_report.xml'
    )

    return 
