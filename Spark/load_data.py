# ============================================================================
# DATA LOADING & PREPROCESSING
# ============================================================================
import pyspark
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
import time
from datetime import datetime


from configuration import SparkConfig ,YelpSchemas


class DataLoader:
    """Handles data loading and preprocessing"""
    
    def __init__(self, spark, data_path="../bigdata-2025-1/data/"):
        self.spark = spark
        self.data_path = data_path
        self.schemas = YelpSchemas()
    
    def load_business_data(self):
        """Load and prepare business data"""
        print("Loading business data...")
        
        business_df = self.spark.read \
            .schema(self.schemas.business_schema()) \
            .json(self.data_path + 'business.json')
        
        
        
        # Trigger cache
        count = business_df.count()
        print(f"Loaded {count:,} businesses")
        
        return business_df
    
    def load_review_data(self):
        """Load and prepare review data"""
        print("Loading review data...")
        
        review_df = self.spark.read \
            .schema(self.schemas.review_schema()) \
            .json(self.data_path + 'review.json')
        
        
        
        count = review_df.count()
        print(f"Loaded {count:,} reviews")
        
        return review_df
    
    def load_user_data(self):
        """Load user data"""
        print("Loading user data...")
        
        user_df = self.spark.read \
            .schema(self.schemas.user_schema()) \
            .json(self.data_path + 'user.json')
        
        count = user_df.count()
        print(f"Loaded {count:,} users")
        
        return user_df


