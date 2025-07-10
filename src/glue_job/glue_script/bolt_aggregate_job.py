
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import boto3
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Accept job parameters from Step Function
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

try:
    print("Glue Aggregation Job...")
except Exception as e:
    print(f"Error during transformation: {e}")
    raise
