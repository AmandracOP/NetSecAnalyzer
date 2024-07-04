from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, desc

# Starting spark session
spark = SparkSession.builder \
    .appName("DNSDataAnalysis") \
    .getOrCreate()

# Reading the data we creaeted in data_processing.py
df = spark.read.parquet("data/processed/processed_dns_data.parquet")

# Anomalies in the data
# For simplicity, I assume high rcode values are anomalies(not necessarily true though)
anomalies = df.filter(col("rcode") > 3)
anomalies.show(truncate=False)

# Data skew
# Checking distribution of ancount
df.groupBy("ancount").count().show()

# Trends in the data
# Average ancount per domain
df.groupBy("domain").agg(mean("ancount").alias("avg_ancount")).show()

# Stoping the spark session
spark.stop()
