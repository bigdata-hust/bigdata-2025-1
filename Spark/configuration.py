import pyspark
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""

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
    """Spark configuration optimized for big data processing"""
    
    @staticmethod
    def create_spark_session():
        """
        Initialize Spark Session with optimized configurations
        """
        spark = SparkSession.builder \
            .appName("Yelp Big Data Analysis System") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "200") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
            .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.sql.files.openCostInBytes", "4194304") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setCheckpointDir("checkpoints/")
        
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
