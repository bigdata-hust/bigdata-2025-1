from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from configuration import SparkConfig, YelpSchemas

class DataLoader:
    """Handles data loading and preprocessing"""

    def __init__(self, spark, data_path="../bigdata-2025-1/data/"):
        self.spark = spark
        self.data_path = data_path
        self.schemas = YelpSchemas()

    def load_business_data(self):
        """Load and prepare business data"""
        print("Loading business data...")

        business_df = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.23.152.231:9092")
                .option("subscribe", "business")
                .option("startingOffsets", "earliest")
                .load()
        )

        business_df = (
            business_df.withColumn("value", col("value").cast("string"))
                .withColumn("value", from_json(col("value"), ArrayType(self.schemas.business_schema())))
                .withColumn("value", explode(col("value")))
                .select("value.*") \
                .withColumn('business_ts' , current_timestamp()) \
                .withWatermark('business_ts' , '10 minutes' )
        )

        

        return business_df

    def load_review_data(self):
        print("Loading review data...")

        review_df = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.23.152.231:9092")
                .option("subscribe", "review")
                .option("startingOffsets", "earliest")
                .load()
        )

        review_df = (
            review_df.withColumn("value", col("value").cast("string"))
                .withColumn("value", from_json(col("value"), ArrayType(self.schemas.review_schema())))
                .withColumn("value", explode(col("value")))
                .select("value.*") \
                .withColumn('review_ts' , current_timestamp()) \
                .withWatermark('review_ts' , '10 minutes' )
        )

        return review_df

    def load_user_data(self):
        print("Loading user data...")

        user_df = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.23.152.231:9092")
                .option("subscribe", "user")
                .option("startingOffsets", "earliest")
                .load()
        )

        user_df = (
            user_df.withColumn("value", col("value").cast("string"))
                .withColumn("value", from_json(col("value"), ArrayType(self.schemas.user_schema())))
                .withColumn("value", explode(col("value")))
                .select("value.*") \
                .withColumn('user_ts' , current_timestamp()) \
                .withWatermark('user_ts' , '10 minutes' )
        )

      
        return user_df
