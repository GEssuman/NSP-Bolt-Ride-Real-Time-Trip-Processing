import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# # Required to use Delta Lake in Glue
# spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Delta table path
delta_path = "s3://nsp-bolt-kpi.amalitech-gke/delta_table_test/"

# Sample schema and data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True)
])

data = [
    (1, "Alice", 25, "alice@example.com"),
    (2, "Bob", 31, "bob_updated@example.com"),     # updated record
    (4, "Diana", 40, "diana@example.com")          # new record
]

df = spark.createDataFrame(data, schema=schema)

try:
    print("Running Delta Lake Upsert Job...")

    # Check if Delta table already exists
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)

        # Perform UPSERT (MERGE INTO)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Upsert completed.")
    else:
        # First time: Write new Delta table
        df.write.format("delta").mode("overwrite").save(delta_path)
        print("Delta table created.")

except Exception as e:
    print(f"Error during transformation: {e}")
    raise
