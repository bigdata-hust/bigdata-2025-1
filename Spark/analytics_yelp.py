# ============================================================================
# ANALYSIS FUNCTIONS - OPTIMIZED FOR BIG DATA
# ============================================================================
import pyspark
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""

<<<<<<< HEAD
from pyspark.sql import SparkSession , Window
=======
from pyspark.sql import SparkSession ,Window
>>>>>>> 1b1e6d05e7b76a86b1031a33f0cfb8700b1ae770
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)

import pyspark.sql.functions as F
import time
import pyspark.sql.functions as F
from datetime import datetime


from configuration import SparkConfig


class YelpAnalytics:
    """Core analytics functions optimized for big data"""
    
    @staticmethod
    def top_selling_products_recent(review_df, business_df, days, top_n):
        print(f"\n{'='*60}")
        print(f"Analysis 1: Top {top_n} Selling Products (Last {days} days)")
        print(f"{'='*60}")
        start_time = time.time()
        
        # Add salt to handle skew
        review_with_salt = review_df.withColumn("salt", (rand() * 10).cast("int"))
        
        # # Filter by date range
        cutoff_date = date_sub(to_timestamp(lit("2022-01-19 00:00:00"), "yyyy-MM-dd HH:mm:ss"), days)
        review_with_salt = review_with_salt.withColumn('date' , to_timestamp(col('date') , 'yyyy-MM-dd HH:mm:ss'))
        recent_reviews = review_with_salt.filter(col("date") >= cutoff_date)
        
        # Stage 1: Salted aggregation
        salted_agg = recent_reviews.groupBy("business_id", "salt" , 'review_ts').agg(
            count("review_id").alias("partial_count"),
            sum("stars").alias("partial_sum_stars"),
            count("stars").alias("partial_count_stars")
        )
        
        # Stage 2: Final aggregation
        business_stats = salted_agg.groupBy("business_id" , 'review_ts').agg(
            sum("partial_count").alias("recent_review_count"),
            (sum("partial_sum_stars") / sum("partial_count_stars")).alias("avg_rating")
        )
        
        # Get top candidates before join
        top_candidates = business_stats \
                        .limit(top_n * 10)  
        #.orderBy(desc("recent_review_count")) \
            
        business_df = business_df.select("business_id", "name", "city", "state", "categories" , 'business_ts') 
        # Broadcast join with business info
        result = top_candidates.join(business_df , "business_id" , 'inner')
        

        
        return result
    
    
    @staticmethod
    def top_stores_by_product_count(business_df, top_n=10):
        """
        2. Cửa hàng bán nhiều sản phẩm nhất (dựa trên categories)
        
        Optimizations:
        - Early null filtering
        - Minimal column selection
        - Efficient string processing
        
        Args:
            business_df: DataFrame chứa dữ liệu business
            top_n: số lượng top cửa hàng
        
        Returns:
            DataFrame với top cửa hàng đa dạng nhất
        """
        print(f"\n{'='*60}")
        print(f"Analysis 2: Top {top_n} Stores by Product Diversity")
        print(f"{'='*60}")
        start_time = time.time()
        
        # Filter and select only needed columns
        business_filtered = business_df \
            .filter(col("categories").isNotNull()) \
            .filter(length(col("categories")) > 0) \
            .select(
                "business_id", "name", "city", "state", 
                "categories", "review_count", "stars" , 'business_ts'
            )
        
        # Count categories
        result = business_filtered.withColumn(
            "category_count",
            size(split(trim(col("categories")), "\\s*,\\s*"))
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "category_count",
            "review_count",
            "stars",
            'business_ts'
        )
        
      
        
        return result
    
    @staticmethod
    def top_rated_products(business_df, review_df, min_reviews=50, top_n=10):
        """
        3. Sản phẩm (doanh nghiệp) đánh giá tích cực nhất
        
        Optimizations:
        - Partitioning by business_id
        - Strategic caching
        - Early filtering by min_reviews
        - Broadcast join
        
        Args:
            business_df: DataFrame chứa dữ liệu business
            review_df: DataFrame chứa dữ liệu review
            min_reviews: số lượng review tối thiểu
            top_n: số lượng top sản phẩm
        
        Returns:
            DataFrame với top sản phẩm có rating cao nhất
        """
        print(f"\n{'='*60}")
        print(f"Analysis 3: Top {top_n} Rated Products (Min {min_reviews} reviews)")
        print(f"{'='*60}")
        start_time = time.time()
        
        # Repartition and cache
        review_partitioned = review_df \
            .select("business_id", "review_id", "stars", "useful" , 'review_ts') 
           
        
        # Aggregate review stats
        business_stats = review_partitioned \
            .filter(col("stars").isNotNull()) \
            .groupBy("business_id" , 'review_ts') \
            .agg(
                count("review_id").alias("total_reviews"),
                avg("stars").alias("avg_review_stars"),
                sum("useful").alias("total_useful")
            )
        
        # Filter by minimum reviews
        qualified = business_stats.filter(col("total_reviews") >= min_reviews)
        
        # Get top candidates
        top_candidates = qualified \
            .limit(top_n * 5)
        
        # Broadcast join
        result = top_candidates.join(
            business_df.select(
                "business_id", "name", "city", "state", "categories", "stars" , 'business_ts'
            ),
            "business_id" , 'inner'
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "total_reviews",
            "avg_review_stars",
            "total_useful",
            col("stars").alias("business_avg_stars") ,
            # 'business_ts' ,
            'review_ts'
        )
        
      
        
        # Cleanup
        review_partitioned.unpersist()
        
       
        return result
    
    @staticmethod
    def top_stores_by_positive_reviews(business_df, review_df, 
                                       positive_threshold=4, top_n=10):
        """
        4. Cửa hàng nhận nhiều đánh giá tích cực nhất
        
        Optimizations:
        - Single-pass aggregation with conditional logic
        - Repartitioning and caching
        - Early filtering
        - Broadcast join
        
        Args:
            business_df: DataFrame chứa dữ liệu business
            review_df: DataFrame chứa dữ liệu review
            positive_threshold: ngưỡng sao tích cực (default: 4)
            top_n: số lượng top cửa hàng
        
        Returns:
            DataFrame với top cửa hàng có nhiều review tích cực nhất
        """
        print(f"\n{'='*60}")
        print(f"Analysis 4: Top {top_n} Stores by Positive Reviews (>= {positive_threshold} stars)")
        print(f"{'='*60}")
        start_time = time.time()
        
        # Repartition and cache
        review_partitioned = review_df \
            .select("business_id", "review_id", "stars", "useful" , 'review_ts') 
            

        
        # Single-pass aggregation with conditional logic
        review_stats = review_partitioned.groupBy("business_id" , 'review_ts').agg(
            # Count positive reviews
            sum(when(col("stars") >= positive_threshold, 1).otherwise(0))
                .alias("positive_review_count"),
            
            # Total review count
            count("review_id").alias("total_review_count"),
            
            # Average stars of positive reviews
            avg(when(col("stars") >= positive_threshold, col("stars")))
                .alias("avg_positive_rating"),
            
            # Total useful votes from positive reviews
            sum(when(col("stars") >= positive_threshold, col("useful")).otherwise(0))
                .alias("total_useful_votes")
        )
        
        # Calculate positive ratio and filter
        review_stats_filtered = review_stats \
            .withColumn(
                "positive_ratio", 
                col("positive_review_count") / col("total_review_count")
            ) \
            .filter(col("positive_review_count") > 0)
        
        # Get top candidates
        top_candidates = review_stats_filtered \
            .limit(top_n * 3)
        
        # Broadcast join
        result = top_candidates.join(
            business_df.select(
                "business_id", "name", "city", "state", "categories" , 'business_ts'
            ),
            "business_id" , 'inner'
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "positive_review_count",
            "total_review_count",
            "positive_ratio",
            "avg_positive_rating",
            "total_useful_votes" ,
            # 'business_ts' , 
            'review_ts'
        )
        

        
        # Cleanup
        review_partitioned.unpersist()
        
    
        return result
    

    # ================================================================
    # 5.Phân tích thời gian cao điểm (review nhiều nhất)
    # ================================================================
    @staticmethod
    def get_peak_hours(review_df):
        """
        Phân tích số lượng review theo năm / tháng / giờ.
        """
        print(f"\n{'='*60}")
        print("Analysis 2: Peak Review Hours (Activity Over Time)")
        print(f"{'='*60}")
        start_time = time.time()

        # Cột date có dạng "yyyy-MM-dd HH:mm:ss"
        df = review_df.withColumn("date_parsed", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

        result = (
            df.groupBy(
                year("date_parsed").alias("year"),
                month("date_parsed").alias("month") ,
                'review_ts'
            )
            .agg(count("review_id").alias("review_count"))
        )

      
        return result

    # ================================================================
    # 6. Top danh mục (category) có nhiều review nhất
    # ================================================================
    @staticmethod
    def get_top_categories(business_df, review_df, top_n=20):
        """
        Phân tích top danh mục (category) bán chạy nhất - dựa trên số lượng review.
        """
        print(f"\n{'='*60}")
        print(f"Analysis 3: Top {top_n} Categories by Review Count")
        print(f"{'='*60}")
        start_time = time.time()

        # Tách categories thành từng dòng riêng
        df_business = business_df.withColumn("category", explode(split(col("categories"), ",\\s*")))

        # Join review với business
        joined = review_df.join(df_business.select("business_id", "category" , 'business_ts'), "business_id" , 'inner')

        # Đếm số lượng review cho từng category
        result = (
            joined.groupBy("category" , 'review_ts' )
            .agg(count("review_id").alias("total_reviews"))
        )

        
        return result

    # ================================================================
    # 7 Thống kê thông tin tất cả cửa hàng
    # ================================================================
    @staticmethod
    def get_store_stats(business_df, review_df):
        """
        Trả về thống kê tổng hợp của tất cả cửa hàng:
        - Tên, danh mục, điểm sao trung bình, tổng số review thực tế,...
        """
        print(f"\n{'='*60}")
        print("Analysis 4: Store Statistics Summary")
        print(f"{'='*60}")
        start_time = time.time()

        # Tính toán lại số lượng review và sao trung bình thực tế
        review_stats = (
            review_df.groupBy("business_id" , 'review_ts')
            .agg(
                count("review_id").alias("actual_review_count"),
                avg("stars").alias("actual_avg_stars")
            )
        )

        # Gộp với thông tin cửa hàng
        result = (
            business_df.join(review_stats, "business_id", "inner")
            .select(
                "business_id",
                "name",
                "city",
                "state",
                "categories",
                "stars",
                "review_count",
                "actual_review_count",
                "actual_avg_stars" ,
                # 'review_ts' ,
                'business_ts'
            )
        )


        return result


    # ================================================================
    # 8. Phân tích cảm xúc đánh giá theo thành phố
    # ================================================================
    @staticmethod
    def yelp_city_sentiment_summary(business_df, review_df, user_df):

        b = business_df.alias("b")
        r = review_df.alias("r")
        u = user_df.alias("u")

        df = (
            r.join(
                b.select("business_id", "city", "business_ts"),
                on="business_id",
                how="inner"
            )
            .join(
                u.select(
                    "user_id",
                    "user_ts",
                    F.col("name").alias("user_name"),
                    F.col("fans").alias("user_fans"),
                    F.col("useful").alias("user_useful")
                ),
                on="user_id",
                how="inner"
            )
        )

        df = df.withWatermark("review_ts", "10 minutes")

        df = df.withColumn(
            "sentiment",
            F.when(F.col("stars") >= 4, "Positive")
            .when(F.col("stars") == 3, "Neutral")
            .otherwise("Negative")
        )

    
        windowed_df = df.withColumn(
            "window",
            F.window("review_ts", "10 minutes")
        )

      
        sentiment_pivot = (
            windowed_df
            .groupBy("window", "city")
            .pivot("sentiment", ["Positive", "Neutral", "Negative"])
            .agg(F.count("review_id"))
            .fillna(0)
        )

        city_metrics = (
            windowed_df
            .groupBy("window", "city")
            .agg(
                F.count("review_id").alias("total_reviews"),
                F.round(F.avg("stars"), 2).alias("avg_stars"),
                F.count("business_id").alias("business_events"),
                F.count("user_id").alias("user_events")
            )
        )

    
        final_df = (
            city_metrics
            .join(sentiment_pivot, ["window", "city"], "left")
            .select(
                "window",
                "city",
                "total_reviews",
                "avg_stars",
                "business_events",
                "user_events",
                "Positive",
                "Neutral",
                "Negative"
            )
        )

        return final_df

