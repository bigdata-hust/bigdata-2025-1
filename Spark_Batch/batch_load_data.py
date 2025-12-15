"""
Data Loader - BATCH MODE
Load data from local files (JSON/CSV) instead of Kafka streams
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from batch_configuration import SparkConfig, YelpSchemas


class DataLoader:
    """Handles data loading from local files (Batch mode)"""

    def __init__(self, spark, data_path=None):
        self.spark = spark
        # Default data path - can be overridden
        self.data_path = data_path or os.getenv("DATA_PATH", "./data/")
        self.schemas = YelpSchemas()

        print(f"\n{'='*60}")
        print("DATA LOADER - BATCH MODE")
        print(f"{'='*60}")
        print(f"Data path: {self.data_path}")
        print(f"{'='*60}\n")

    def load_business_data(self):
        """Load business data from local JSON file"""
        print("Loading business data...")

        business_path = os.path.join(self.data_path, "business.json")

        if not os.path.exists(business_path):
            raise FileNotFoundError(
                f"Business data not found at: {business_path}\n"
                f"Please ensure business.json exists in {self.data_path}"
            )

        # Read JSON with explicit schema
        business_df = (
            self.spark.read
            .schema(self.schemas.business_schema())
            .json(business_path)
        )

        # Cache for reuse
        business_df.cache()
        count = business_df.count()

        print(f"✓ Loaded {count:,} businesses from {business_path}")

        return business_df

    def load_review_data(self):
        """Load review data from local JSON file"""
        print("Loading review data...")

        review_path = os.path.join(self.data_path, "review.json")

        if not os.path.exists(review_path):
            raise FileNotFoundError(
                f"Review data not found at: {review_path}\n"
                f"Please ensure review.json exists in {self.data_path}"
            )

        # Read JSON with explicit schema
        review_df = (
            self.spark.read
            .schema(self.schemas.review_schema())
            .json(review_path)
        )

        # Cache for reuse
        review_df.cache()
        count = review_df.count()

        print(f"✓ Loaded {count:,} reviews from {review_path}")

        return review_df

    def load_user_data(self):
        """Load user data from local JSON file (optional)"""
        print("Loading user data...")

        user_path = os.path.join(self.data_path, "user.json")

        if not os.path.exists(user_path):
            print(f"⚠ User data not found at: {user_path} (skipping)")
            return None

        # Read JSON with explicit schema
        user_df = (
            self.spark.read
            .schema(self.schemas.user_schema())
            .json(user_path)
        )

        # Cache for reuse
        user_df.cache()
        count = user_df.count()

        print(f"✓ Loaded {count:,} users from {user_path}")

        return user_df

    def load_all_data(self):
        """
        Load all datasets at once

        Returns:
            tuple: (business_df, review_df, user_df)
        """
        business_df = self.load_business_data()
        review_df = self.load_review_data()
        user_df = self.load_user_data()  # Optional

        print(f"\n{'='*60}")
        print("✓ ALL DATA LOADED SUCCESSFULLY")
        print(f"{'='*60}\n")

        return business_df, review_df, user_df
