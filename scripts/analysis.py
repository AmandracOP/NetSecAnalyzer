from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DNSDataAnalysis") \
    .getOrCreate()

# Read the processed data
df = spark.read.parquet("data/processed/processed_dns_data.parquet")

# Anomalies in the data
# For simplicity, let's assume high rcode values are anomalies
anomalies = df.filter(col("rcode") > 3)
anomalies.show(truncate=False)

# Data skew
# Check the distribution of ancount
df.groupBy("ancount").count().show()

# Trends in the data
# Average ancount per domain
df.groupBy("domain").agg(mean("ancount").alias("avg_ancount")).show()

# Stop the Spark session
spark.stop()
