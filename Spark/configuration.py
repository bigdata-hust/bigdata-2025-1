
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""
import os
import builtins
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType , LongType
)
import time
from datetime import datetime




class SparkConfig:
    @staticmethod
    def _get_env(key: str, default: str) -> str:
        return os.environ.get(key, default)



    @staticmethod
    def create_spark_session():
        # Environment variables
        spark_master = SparkConfig._get_env("SPARK_MASTER_URL", "spark://spark-master:7077")
        hdfs_uri = SparkConfig._get_env("HDFS_URI", "hdfs://127.0.0.1:9000")
        checkpoint_dir = SparkConfig._get_env("CHECKPOINT_DIR", f"{hdfs_uri}/checkpoints")
        executor_cores = SparkConfig._get_env("SPARK_EXECUTOR_CORES", "1")
        
        # Compute partitions

        builder = (
            SparkSession.builder
            .appName("Yelp Big Data Analysis System")
            .master(spark_master)
            
            # ============ HADOOP/HDFS CONFIG ============
            .config("spark.hadoop.fs.defaultFS", hdfs_uri)
            
            # ============ EXECUTOR CONFIG ============
            .config("spark.executor.instances", "1")
            .config("spark.executor.cores", executor_cores)
            .config("spark.executor.memory", SparkConfig._get_env("SPARK_EXECUTOR_MEMORY", "1g"))
            
            # ============ DRIVER CONFIG ============
            .config("spark.driver.memory", SparkConfig._get_env("SPARK_DRIVER_MEMORY", "700m"))
            .config("spark.driver.port", "7078")
            .config("spark.driver.blockManager.port", "7079")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.host", SparkConfig._get_env("SPARK_DRIVER_HOST", "0.0.0.0"))
            
            # ============ MEMORY OVERHEAD (CRITICAL!) ============
            # Overhead = max(384MB, 10% of heap memory)
            # For 1g executor: need 512MB overhead for shuffle operations
            .config("spark.executor.memoryOverhead", 
                    SparkConfig._get_env("SPARK_EXECUTOR_MEMORY_OVERHEAD", "512m"))
            .config("spark.driver.memoryOverhead", "300m")
            
            # ============ MEMORY MANAGEMENT ============
            # Total JVM memory = execution + storage
            .config("spark.memory.fraction", "0.7")  # 60% of heap for Spark
            # Of that 60%, how much for storage vs execution
            .config("spark.memory.storageFraction", "0.3")  # 40% for cache, 60% for shuffle
            .config("spark.memory.offHeap.enabled", "false")  # Disable to save memory
            
            # ============ SHUFFLE OPTIMIZATION ============
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.shuffle.compress", "true")
            .config("spark.shuffle.spill.compress", "true")
            .config("spark.shuffle.file.buffer", "32k")  # Default 32k, reduce if needed
            .config("spark.reducer.maxSizeInFlight", "32m")  # Max size to fetch at once
            .config("spark.shuffle.sort.bypassMergeThreshold", "200")  # Optimize small shuffles
            
            # ============ SHUFFLE RETRY & TIMEOUT ============
            .config("spark.shuffle.io.maxRetries", "5")
            .config("spark.shuffle.io.retryWait", "30s")
            .config("spark.shuffle.io.connectionTimeout", "120s")
            
            # ============ NETWORK TIMEOUT ============
            .config("spark.network.timeout", 
                    SparkConfig._get_env("SPARK_NETWORK_TIMEOUT", "600s"))
            .config("spark.executor.heartbeatInterval",
                    SparkConfig._get_env("SPARK_EXECUTOR_HEARTBEAT_INTERVAL", "60s"))
            .config("spark.rpc.askTimeout", 
                    SparkConfig._get_env("SPARK_RPC_ASK_TIMEOUT", "600s"))
            .config("spark.rpc.lookupTimeout",
                    SparkConfig._get_env("SPARK_RPC_LOOKUP_TIMEOUT", "600s"))
            
            # ============ TASK & STAGE FAILURES ============
            .config("spark.task.maxFailures", 
                    SparkConfig._get_env("SPARK_TASK_MAX_FAILURES", "4"))
            .config("spark.stage.maxConsecutiveAttempts",
                    SparkConfig._get_env("SPARK_STAGE_MAX_CONSECUTIVE_ATTEMPTS", "4"))

            # ============ ADAPTIVE QUERY EXECUTION (AQE) ============
            # Auto-optimize joins and shuffles at runtime
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "134217728")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
            
            # ============ STREAMING CONFIG ============
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.backpressure.initialRate", "1000")
            .config("spark.streaming.kafka.maxRatePerPartition", "500")  # Throttle ingestion
            
            # ============ SERIALIZATION ============
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "64m")
            
            # ============ BROADCAST ============
            .config("spark.sql.autoBroadcastJoinThreshold", "10MB")  # Small tables
            
            # ============ DYNAMIC ALLOCATION ============
            .config("spark.dynamicAllocation.enabled", 
                    SparkConfig._get_env("SPARK_DYNAMIC_ALLOCATION", "false"))
            .config("spark.shuffle.service.enabled", 
                    SparkConfig._get_env("SPARK_SHUFFLE_SERVICE", "false"))
            
            # ============ MISC ============
            .config("spark.shuffle.push.enabled", "false")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
            
            .config('spark.streaming.stopGracefullyOnShutdown' , True)
            

            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        )

        # Add external packages (Kafka, Elasticsearch, etc.)
        jars = SparkConfig._get_env("SPARK_JARS_PACKAGES", "")
        if jars:
            builder = builder.config("spark.jars.packages", jars)

        # Create session
        spark = builder.getOrCreate()
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
        
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