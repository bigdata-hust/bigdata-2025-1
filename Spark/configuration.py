import pyspark
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType , LongType
)
import time
from datetime import datetime




# ============================================================================
# CONFIGURATION & INITIALIZATION
# ============================================================================
class SparkConfig:
    def create_spark_session():
        """
        Initialize Spark Session with optimized configurations for streaming
        """
        # Đảm bảo thư mục checkpoints tồn tại
        os.makedirs("checkpoints", exist_ok=True)

        spark = (
            SparkSession.builder
            .appName("Yelp Big Data Analysis System")

            # ---- MEMORY ----
            .config("spark.driver.memory", "16g")      
            .config("spark.executor.memory", "4g")
            .config("spark.memory.fraction", "0.6")

            # ---- STREAMING ----
            .config("spark.sql.shuffle.partitions", "20")   
            .config("spark.default.parallelism", "20")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")

            # ---- SERIALIZER ----
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "512m")

            # ---- OPTIONAL ----
            .config("spark.sql.streaming.stateStore.providerClass", 
                    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .config('spark.streaming.stopGracefullyOnShutdown' , True)

            # ---- KAFKA - MONGODB - ELASTICSEARCH ----
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1," 
                'org.mongodb.spark:mongo-spark-connector_2.13:10.5.0,' 
                'org.elasticsearch:elasticsearch-spark-30_2.13:8.14.3'
            )
            .getOrCreate() 
            # .config('spark.mongodb.write.connection.uri' , 'mongodb://mongodb:27017')
            
                    
        )

        # ✅ Đặt checkpointDir an toàn (dưới spark context)
        spark.sparkContext.setCheckpointDir(os.path.abspath("checkpoints"))
        spark.sparkContext.setLogLevel("WARN")

    
  
        return spark
# ============================================================================
# DATA SCHEMAS
# ============================================================================

class YelpSchemas:
    """Explicit schemas for better performance"""
    
    @staticmethod
    def business_schema():
        return StructType([
            StructField("address", StringType(), True),
            StructField("attributes", StringType(), True),  
            StructField("business_id", StringType(), False),
            StructField("categories", StringType(), True),
            StructField("city", StringType(), True),
            StructField("hours", StringType(), True),  
            StructField("is_open", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("name", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("review_count", LongType(), True),
            StructField("stars", DoubleType(), True),
            StructField("state", StringType(), True)
        ])
    
    @staticmethod
    def review_schema():
        return StructType([
            StructField("business_id", StringType(), True),
            StructField("cool", LongType(), True),
            StructField("date", StringType(), True),
            StructField("funny", LongType(), True),
            StructField("review_id", StringType(), True),
            StructField("stars", DoubleType(), True),
            StructField("text", StringType(), True),
            StructField("useful", LongType(), True),
            StructField("user_id", StringType(), True)
        ])
        
    @staticmethod
    def user_schema():
        return StructType([
            StructField("average_stars", DoubleType(), True),
            StructField("compliment_cool", LongType(), True),
            StructField("compliment_cute", LongType(), True),
            StructField("compliment_funny", LongType(), True),
            StructField("compliment_hot", LongType(), True),
            StructField("compliment_list", LongType(), True),
            StructField("compliment_more", LongType(), True),
            StructField("compliment_note", LongType(), True),
            StructField("compliment_photos", LongType(), True),
            StructField("compliment_plain", LongType(), True),
            StructField("compliment_profile", LongType(), True),
            StructField("compliment_writer", LongType(), True),
            StructField("cool", LongType(), True),
            StructField("elite", StringType(), True),
            StructField("fans", LongType(), True),
            StructField("friends", StringType(), True),
            StructField("funny", LongType(), True),
            StructField("name", StringType(), True),
            StructField("review_count", LongType(), True),
            StructField("useful", LongType(), True),
            StructField("user_id", StringType(), False),
            StructField("yelping_since", StringType(), True)
        ])
