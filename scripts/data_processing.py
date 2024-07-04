from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Starting spark session
spark = SparkSession.builder \
    .appName("DNSDataProcessing") \
    .getOrCreate()

# Reading parquet file
df = spark.read.parquet("data/raw/dummy_dns_data.parquet")

# Viewing the schema
df.printSchema()

# Viewing rows
df.show(truncate=False)

# Saving the processesd data
df.write.mode("overwrite").parquet("data/processed/processed_dns_data.parquet")

# Stoping spark session
spark.stop()
