from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class DataAggregator:
    def __init__(self, df):
        self.df = df

    def aggregate(self, by_field):
        return self.df.groupBy(by_field).count()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DNSDataAggregation") \
        .getOrCreate()

    # Read the processed data
    df = spark.read.parquet("data/processed/processed_dns_data.parquet")

    # Initialize the aggregator
    aggregator = DataAggregator(df)

    # Aggregate by domain
    domain_aggregation = aggregator.aggregate("domain")
    domain_aggregation.show()

    # Aggregate by ancount
    ancount_aggregation = aggregator.aggregate("ancount")
    ancount_aggregation.show()

    # Stop the Spark session
    spark.stop()