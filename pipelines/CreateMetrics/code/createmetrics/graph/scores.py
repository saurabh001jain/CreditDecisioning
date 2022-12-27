from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from createmetrics.config.ConfigStore import *
from createmetrics.udfs.UDFs import *

def scores(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ID", StringType(), True), StructField("Amount.Requested", StringType(), True), StructField("Amount.Funded.By.Investors", StringType(), True), StructField("Loan.Length", StringType(), True), StructField("Loan.Purpose", StringType(), True), StructField("Debt.To.Income.Ratio", StringType(), True), StructField("State", StringType(), True), StructField("Home.Ownership", StringType(), True), StructField("Monthly.Income", StringType(), True), StructField("FICORange", StringType(), True), StructField("Open.CREDIT.Lines", StringType(), True), StructField("Revolving.CREDIT.Balance", StringType(), True), StructField("Inquiries.in.the.Last.6.Months", StringType(), True), StructField("Employment.Length", StringType(), True), StructField("date_of_FICO_score", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/data/loan_data_test_formatted.csv")
