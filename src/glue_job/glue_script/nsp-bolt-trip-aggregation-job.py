import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime
import json
import boto3



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# # Required to use Delta Lake in Glue
# spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Delta table path
delta_path = "s3://nsp-bolt-kpi.amalitech-gke/delta_main/"

# === Config ===
region = "eu-north-1"
sqs_queue_url = "https://sqs.eu-north-1.amazonaws.com/309797288544/nsp_complete_bolt_trip"

sqs = boto3.client("sqs", region_name=region)


# === Receive SQS messages ===
response = sqs.receive_message(
    QueueUrl=sqs_queue_url,
    MaxNumberOfMessages=10,
    WaitTimeSeconds=5
)


messages = response.get("Messages", [])
if not messages:
    raise Exception("No messages found in SQS.")

trip_records = []
receipt_handles = []

for msg in messages:
    try:
        record = json.loads(msg["Body"])
        trip_records.append(record)
        receipt_handles.append(msg["ReceiptHandle"])
    except Exception as e:
        print("Failed to parse message:", e)

if not trip_records:
    raise Exception("No valid trip records found.")


schema = StructType([
    StructField("trip_id", StringType()),
    StructField("fare_amount", StringType()),
    StructField("estimated_fare_amount", StringType()),
    StructField("estimated_dropoff_datetime", StringType()),
    StructField("pickup_location_id", StringType()),
    StructField("pickup_datetime", StringType()),
    StructField("dropoff_location_id", StringType()),
    StructField("rate_code", StringType()),
    StructField("trip_distance", StringType()),
    StructField("payment_type", StringType()),
    StructField("trip_type", StringType()),
    StructField("vendor_id", StringType()),
    StructField("tip_amount", StringType()),
    StructField("dropoff_datetime", StringType()),
])



raw_df = spark.createDataFrame(trip_records, schema)

# Cast fare_amount and pickup_datetime
df = raw_df.withColumn("fare", F.col("fare_amount").cast("double")) \
           .withColumn("pickup_date", F.to_date("pickup_datetime"))

# === Step 3: Compute Daily KPIs ===
kpi_df = df.groupBy("pickup_date").agg(
    F.sum("fare").alias("total_fare"),
    F.count("*").alias("count_trips"),
    F.avg("fare").alias("average_fare"),
    F.max("fare").alias("max_fare"),
    F.min("fare").alias("min_fare")
)
# === Step 4: Upsert into Delta KPI Table ===
try:
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)

        (
        # Perform the upsert with custom logic
        delta_table.alias("target").merge(
            kpi_df.alias("source"),
            "target.pickup_date = source.pickup_date"
        ).whenMatchedUpdate(set={
            "total_fare": "target.total_fare + source.total_fare",
            "count_trips": "target.count_trips + source.count_trips",
            "average_fare": "(target.average_fare * target.count_trips + source.average_fare * source.count_trips) / (target.count_trips + source.count_trips)",
            "max_fare": "GREATEST(target.max_fare, source.max_fare)",
            "min_fare": "LEAST(target.min_fare, source.min_fare)"
        }).whenNotMatchedInsertAll().execute()
        )
        print("Incremental KPI upsert successful.")
    else:
        kpi_df.write.format("delta") \
            .partitionBy("pickup_date") \
            .mode("overwrite") \
            .save(delta_path)
        print("Delta KPI table created.")

except Exception as e:
    print("Delta Upsert Error:", e)
    raise

# === Step 5: Delete Processed Messages from SQS ===
for handle in receipt_handles:
    sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=handle)
