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
    @staticmethod
    def _get_env(key: str, default: str) -> str:
        """Lấy biến môi trường hoặc dùng giá trị mặc định"""
        return os.environ.get(key, default)

    @staticmethod
    def create_spark_session():
        # --- 1. LẤY CẤU HÌNH KẾT NỐI TỪ MÔI TRƯỜNG ---
        spark_master = SparkConfig._get_env("SPARK_MASTER_URL", "spark://spark-master:7077")
        hdfs_uri = SparkConfig._get_env("HDFS_URI", "hdfs://127.0.0.1:9000")

        # --- 2. KHỞI TẠO BUILDER ---
        builder = (
            SparkSession.builder
            .appName("Yelp Big Data Analysis System")
            .master(spark_master) # Ép dùng Standalone, không dùng YARN nên sẽ hết lỗi
            
            # ---- HADOOP / HDFS CONFIG ----
            .config("spark.hadoop.fs.defaultFS", hdfs_uri)
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

            # ---- MEMORY (Giữ nguyên cấu hình cũ của bạn) ----
             .config("spark.driver.memory", "8g")

            .config("spark.executor.memory", "24g")


            .config("spark.executor.cores", "4")


            .config("spark.executor.memoryOverhead", "8g")


            .config("spark.executor.instances", "2")

            .config("spark.dynamicAllocation.enabled", "true") 


            .config("spark.shuffle.service.enabled", "false")
            .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
            .config(
                "spark.executor.extraJavaOptions",
                "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"
            )
            # ---- NETWORK / BINDING (Quan trọng để Cluster thấy Driver) ----
            .config("spark.driver.port", "7078")
            .config("spark.driver.blockManager.port", "7079")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.host", SparkConfig._get_env("SPARK_DRIVER_HOST", "0.0.0.0"))
            .config("spark.driver.maxResultSize", "2g")
            # ---- STREAMING (Giữ nguyên cấu hình cũ của bạn) ----
            .config("spark.sql.shuffle.partitions", "8")   
            .config("spark.default.parallelism", "8")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.adaptive.enabled", "true")
            
            
            
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")


            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            # ---- SERIALIZER (Giữ nguyên cấu hình cũ của bạn) ----
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "512m")
            .config("spark.sql.streaming.statefulOperator.allowMultiple", "false")
            # ---- PACKAGES (Kafka, ES) ----
        )
        jars = SparkConfig._get_env("SPARK_JARS_PACKAGES", "")
        if jars:
            builder = builder.config("spark.jars.packages", jars)

        # --- 3. TẠO SESSION VÀ CẤU HÌNH CONTEXT ---
        spark = builder.getOrCreate()
        
        # Đặt checkpointDir lên HDFS để an toàn hơn so với local
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
