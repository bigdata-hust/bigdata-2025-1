from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from configuration import SparkConfig, YelpSchemas
import os

class DataLoader:
    """Handles data loading and preprocessing"""

    def __init__(self, spark, data_path=None):
        self.spark = spark
        self.data_path = data_path or os.getenv("DATA_PATH", "../bigdata-2025-1/data/")
        self.kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.schemas = YelpSchemas()

    def load_business_data(self):
        """Load and prepare business data"""
        print("Loading business data...")

        business_df = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_broker)
                .option("subscribe", "business")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss" , "false")
                .option("maxOffsetsPerTrigger", 5000)
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
                .option("kafka.bootstrap.servers", self.kafka_broker)
                .option("subscribe", "review")
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", 5000)
                .option("failOnDataLoss" , "false")
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
                .option("kafka.bootstrap.servers", self.kafka_broker)
                .option("subscribe", "user")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss" , "false")
                .option("maxOffsetsPerTrigger", 5000)
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
