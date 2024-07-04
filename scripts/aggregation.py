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

    # Reading the processed data
    df = spark.read.parquet("data/processed/processed_dns_data.parquet")

    # starting the aggregator
    aggregator = DataAggregator(df)

    # Aggregating by domain
    domain_aggregation = aggregator.aggregate("domain")
    domain_aggregation.show()

    # Aggregating by ancount
    ancount_aggregation = aggregator.aggregate("ancount")
    ancount_aggregation.show()

    # Stoping spark session
    spark.stop()
