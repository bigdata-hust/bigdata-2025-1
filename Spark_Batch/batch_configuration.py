"""
Yelp Big Data Analysis System - BATCH MODE
Optimized PySpark Configuration for Local Processing
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType, LongType
)


# ============================================================================
# CONFIGURATION & INITIALIZATION
# ============================================================================
class SparkConfig:
    """Spark configuration for batch processing (no streaming)"""

    @staticmethod
    def create_spark_session():
        """
        Initialize Spark Session with optimized configurations for batch processing
        """
        spark = (
            SparkSession.builder
            .appName("Yelp Big Data Analysis System - Batch Mode")

            # ---- MEMORY ----
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "4g")
            .config("spark.memory.fraction", "0.6")

            # ---- PERFORMANCE ----
            .config("spark.sql.shuffle.partitions", "20")
            .config("spark.default.parallelism", "20")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # ---- SERIALIZER ----
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "512m")

            # ---- UI ----
            .config("spark.ui.port", "4040")

            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        print("✓ Spark Session created successfully (Batch Mode)")
        print(f"  - Spark Version: {spark.version}")
        print(f"  - Driver Memory: 8g")
        print(f"  - Executor Memory: 4g")
        print(f"  - Spark UI: http://localhost:4040")

        return spark


# ============================================================================
# DATA SCHEMAS
# ============================================================================
class YelpSchemas:
    """Explicit schemas for better performance and type safety"""

    @staticmethod
    def business_schema():
        """Schema for business.json"""
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
        """Schema for review.json"""
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
        """Schema for user.json"""
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
