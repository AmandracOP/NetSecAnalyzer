from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DNSDataProcessing") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("data/raw/dummy_dns_data.parquet")

# Show the schema
df.printSchema()

# Show a few rows
df.show(truncate=False)

# Save the processed data
df.write.mode("overwrite").parquet("data/processed/processed_dns_data.parquet")

# Stop the Spark session
spark.stop()
