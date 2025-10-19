# config/spark_config.py
from pyspark.sql import SparkSession

def get_spark_session(app_name="YelpAnalysis"):
    """
    Tạo SparkSession với cấu hình tối ưu cho local hoặc cluster.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark 
