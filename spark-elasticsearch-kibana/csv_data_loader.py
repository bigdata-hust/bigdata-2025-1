"""
CSV Data Loader for Yelp Dataset
Load và preprocess dữ liệu từ CSV files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class CSVDataLoader:
    """
    Load dữ liệu Yelp từ CSV files
    Thích hợp cho dữ liệu đã được xử lý và lưu dưới dạng CSV
    """
    
    def __init__(self, spark, data_path="../processed_data/"):
        """
        Args:
            spark: SparkSession
            data_path: Đường dẫn đến thư mục chứa CSV files
        """
        self.spark = spark
        self.data_path = data_path
        print(f"Data path: {self.data_path}")
    
    def load_business_data(self):
        """
        Load business data từ business.csv
        
        Returns:
            DataFrame với business data
        """
        print("\n" + "="*60)
        print("Loading business data from CSV...")
        print("="*60)
        
        # Đọc CSV với infer schema
        business_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.data_path + "business.csv")
        
        # Hiển thị schema để verify
        print("\nBusiness Schema:")
        business_df.printSchema()
        
        # Cast các cột cần thiết
        business_df = business_df \
            .withColumn("latitude", col("latitude").cast(DoubleType())) \
            .withColumn("longitude", col("longitude").cast(DoubleType())) \
            .withColumn("stars", col("stars").cast(DoubleType())) \
            .withColumn("review_count", col("review_count").cast(IntegerType())) \
            .withColumn("is_open", col("is_open").cast(IntegerType()))
        
        # Repartition và cache
        business_df = business_df \
            .repartition(100, "business_id") \
            .cache()
        
        # Trigger cache và đếm
        count = business_df.count()
        print(f"✓ Loaded {count:,} businesses")
        
        # Show sample
        print("\nSample business data:")
        business_df.show(3, truncate=False)
        
        return business_df
    
    def load_review_data(self):
        """
        Load review data từ review_combined_1.csv
        
        Returns:
            DataFrame với review data
        """
        print("\n" + "="*60)
        print("Loading review data from CSV...")
        print("="*60)
        
        # Đọc CSV
        review_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.data_path + "review_combined_1.csv")
        
        # Hiển thị schema
        print("\nReview Schema:")
        review_df.printSchema()
        
        # Cast và preprocess
        review_df = review_df \
            .withColumn("stars", col("stars").cast(IntegerType())) \
            .withColumn("useful", col("useful").cast(IntegerType()))
        
        # Xử lý date - detect format và convert
        # Thử nhiều format phổ biến
        review_df = review_df.withColumn(
            "review_date",
            coalesce(
                to_date(col("date"), "yyyy-MM-dd HH:mm:ss"),
                to_date(col("date"), "yyyy-MM-dd"),
                to_date(col("date"), "MM/dd/yyyy"),
                to_date(col("date"), "dd/MM/yyyy")
            )
        )
        
        # Thêm các cột time-based
        review_df = review_df \
            .withColumn("review_timestamp", unix_timestamp(col("review_date"))) \
            .withColumn("review_year", year(col("review_date"))) \
            .withColumn("review_month", month(col("review_date")))
        
        # Repartition by business_id for joins
        review_df = review_df.repartition(200, "business_id")
        
        count = review_df.count()
        print(f"✓ Loaded {count:,} reviews")
        
        # Show sample
        print("\nSample review data:")
        review_df.show(3, truncate=False)
        
        return review_df
    
    def load_user_data(self):
        """
        Load user data từ user.csv
        
        Returns:
            DataFrame với user data
        """
        print("\n" + "="*60)
        print("Loading user data from CSV...")
        print("="*60)
        
        # Đọc CSV
        user_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.data_path + "user.csv")
        
        # Hiển thị schema
        print("\nUser Schema:")
        user_df.printSchema()
        
        # Cast các cột
        user_df = user_df \
            .withColumn("review_count", col("review_count").cast(IntegerType())) \
            .withColumn("useful", col("useful").cast(IntegerType())) \
            .withColumn("fans", col("fans").cast(IntegerType())) \
            .withColumn("average_stars", col("average_stars").cast(DoubleType()))
        
        count = user_df.count()
        print(f"✓ Loaded {count:,} users")
        
        # Show sample
        print("\nSample user data:")
        user_df.show(3, truncate=False)
        
        return user_df
    
    def validate_data(self, business_df, review_df, user_df=None):
        """
        Validate dữ liệu đã load
        
        Args:
            business_df: Business DataFrame
            review_df: Review DataFrame
            user_df: User DataFrame (optional)
        
        Returns:
            Boolean - True nếu data hợp lệ
        """
        print("\n" + "="*60)
        print("Validating data...")
        print("="*60)
        
        issues = []
        
        # Check null business_id
        null_business = business_df.filter(col("business_id").isNull()).count()
        if null_business > 0:
            issues.append(f"Found {null_business} null business_id in business data")
        
        # Check null review_id
        null_review = review_df.filter(col("review_id").isNull()).count()
        if null_review > 0:
            issues.append(f"Found {null_review} null review_id in review data")
        
        # Check date parsing
        null_dates = review_df.filter(col("review_date").isNull()).count()
        if null_dates > 0:
            print(f"⚠️  Warning: {null_dates} reviews have unparseable dates")
        
        # Check join compatibility
        review_businesses = review_df.select("business_id").distinct().count()
        actual_businesses = business_df.count()
        print(f"\nData Statistics:")
        print(f"  Total businesses: {actual_businesses:,}")
        print(f"  Businesses with reviews: {review_businesses:,}")
        print(f"  Total reviews: {review_df.count():,}")
        
        if user_df:
            print(f"  Total users: {user_df.count():,}")
        
        if issues:
            print("\n⚠️  Data validation issues:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        else:
            print("\n✓ Data validation passed!")
            return True


class YelpAnalyticsPipeline:
    """
    Main pipeline class kết hợp data loading và analytics
    Compatible với CSV data
    """
    
    def __init__(self, spark, data_path="../processed_data/"):
        """
        Args:
            spark: SparkSession
            data_path: Đường dẫn đến thư mục CSV
        """
        self.spark = spark
        self.data_loader = CSVDataLoader(spark, data_path)
        self.business_df = None
        self.review_df = None
        self.user_df = None
        self.results = {}
    
    def load_all_data(self, validate=True):
        """
        Load tất cả data files
        
        Args:
            validate: Có validate data sau khi load không
        
        Returns:
            Boolean - True nếu load thành công
        """
        try:
            self.business_df = self.data_loader.load_business_data()
            self.review_df = self.data_loader.load_review_data()
            
            # User data optional
            try:
                self.user_df = self.data_loader.load_user_data()
            except Exception as e:
                print(f"⚠️  Could not load user data: {str(e)}")
                print("Continuing without user data...")
            
            if validate:
                return self.data_loader.validate_data(
                    self.business_df, 
                    self.review_df, 
                    self.user_df
                )
            
            return True
            
        except Exception as e:
            print(f"\n✗ Error loading data: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_dataframes(self):
        """
        Get các DataFrames đã load
        
        Returns:
            Tuple (business_df, review_df, user_df)
        """
        return self.business_df, self.review_df, self.user_df
