# ============================================================================
# ANALYSIS FUNCTIONS - OPTIMIZED FOR BIG DATA
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


from configuration import SparkConfig


class YelpAnalytics:
    """Core analytics functions optimized for big data"""
    
    @staticmethod
    def top_selling_products_recent(review_df, business_df, days=90, top_n=10):
        """
        1. Top sản phẩm (doanh nghiệp) bán chạy nhất trong khoảng thời gian gần
        
        Optimizations:
        - Salting to handle data skew
        - Two-stage aggregation
        - Broadcast join for business info
        - Early filtering and limiting
        
        Args:
            review_df: DataFrame chứa dữ liệu review (đã preprocess dates)
            business_df: DataFrame chứa dữ liệu business
            days: số ngày gần đây cần phân tích
            top_n: số lượng top sản phẩm
        
        Returns:
            DataFrame với top sản phẩm bán chạy nhất
        """
        print(f"\n{'='*60}")
        print(f"Analysis 1: Top {top_n} Selling Products (Last {days} days)")
        print(f"{'='*60}")
        start_time = time.time()
        
        # Add salt to handle skew
        review_with_salt = review_df.withColumn("salt", (rand() * 10).cast("int"))
        
        # Filter by date range
        cutoff_date = to_date(lit('2022-01-19') , 'yyyy-MM-dd') - lit(90)
        review_with_salt = review_with_salt.withColumn('date' , to_date(col('date') , 'yyyy-MM-dd HH:mm:ss'))
        recent_reviews = review_with_salt.filter(col("date") >= cutoff_date)
        
        # Stage 1: Salted aggregation
        salted_agg = recent_reviews.groupBy("business_id", "salt").agg(
            count("review_id").alias("partial_count"),
            sum("stars").alias("partial_sum_stars"),
            count("stars").alias("partial_count_stars")
        )
        
        # Stage 2: Final aggregation
        business_stats = salted_agg.groupBy("business_id").agg(
            sum("partial_count").alias("recent_review_count"),
            (sum("partial_sum_stars") / sum("partial_count_stars")).alias("avg_rating")
        )
        
        # Get top candidates before join
        top_candidates = business_stats \
            .orderBy(desc("recent_review_count")) \
            .limit(top_n * 10)
        
        # Broadcast join with business info
        result = top_candidates.join(
            broadcast(business_df.select(
                "business_id", "name", "city", "state", "categories"
            )),
            "business_id"
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "recent_review_count",
            "avg_rating"
        ).orderBy(desc("recent_review_count")).limit(top_n)
        
        # Materialize result
        result_count = result.count()
        
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} results")
        
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
                "categories", "review_count", "stars"
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
            "stars"
        ).orderBy(
            desc("category_count"), 
            desc("review_count")
        ).limit(top_n)
        
        result_count = result.count()
        
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} results")
        
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
            .select("business_id", "review_id", "stars", "useful") \
            .repartition(200, "business_id") \
            .cache()
        
        # Aggregate review stats
        business_stats = review_partitioned \
            .filter(col("stars").isNotNull()) \
            .groupBy("business_id") \
            .agg(
                count("review_id").alias("total_reviews"),
                avg("stars").alias("avg_review_stars"),
                sum("useful").alias("total_useful")
            )
        
        # Filter by minimum reviews
        qualified = business_stats.filter(col("total_reviews") >= min_reviews)
        
        # Get top candidates
        top_candidates = qualified \
            .orderBy(desc("avg_review_stars"), desc("total_reviews")) \
            .limit(top_n * 5)
        
        # Broadcast join
        result = top_candidates.join(
            broadcast(business_df.select(
                "business_id", "name", "city", "state", "categories", "stars"
            )),
            "business_id"
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "total_reviews",
            "avg_review_stars",
            "total_useful",
            col("stars").alias("business_avg_stars")
        ).orderBy(
            desc("avg_review_stars"), 
            desc("total_reviews")
        ).limit(top_n)
        
        result_count = result.count()
        
        # Cleanup
        review_partitioned.unpersist()
        
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} results")
        
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
            .select("business_id", "review_id", "stars", "useful") \
            .repartition(200, "business_id") \
            .cache()
        
        # Single-pass aggregation with conditional logic
        review_stats = review_partitioned.groupBy("business_id").agg(
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
            .orderBy(desc("positive_review_count"), desc("positive_ratio")) \
            .limit(top_n * 3)
        
        # Broadcast join
        result = top_candidates.join(
            broadcast(business_df.select(
                "business_id", "name", "city", "state", "categories"
            )),
            "business_id"
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
            "total_useful_votes"
        ).orderBy(
            desc("positive_review_count"), 
            desc("positive_ratio")
        ).limit(top_n)
        
        result_count = result.count()
        
        # Cleanup
        review_partitioned.unpersist()
        
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} results")
        
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
        df = review_df.withColumn("date_parsed", to_date(col("date"), "yyyy-MM-dd HH:mm:ss"))

        result = (
            df.groupBy(
                year("date_parsed").alias("year"),
                month("date_parsed").alias("month")
            )
            .agg(count("review_id").alias("review_count"))
            .orderBy(desc("review_count"))
        )

        result_count = result.count()
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} time groups")
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
        joined = review_df.join(broadcast(df_business.select("business_id", "category")), "business_id")

        # Đếm số lượng review cho từng category
        result = (
            joined.groupBy("category")
            .agg(count("review_id").alias("total_reviews"))
            .orderBy(desc("total_reviews"))
            .limit(top_n)
        )

        result_count = result.count()
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} categories")
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
            review_df.groupBy("business_id")
            .agg(
                count("review_id").alias("actual_review_count"),
                avg("stars").alias("actual_avg_stars")
            )
        )

        # Gộp với thông tin cửa hàng
        result = (
            business_df.join(broadcast(review_stats), "business_id", "left")
            .select(
                "business_id",
                "name",
                "city",
                "state",
                "categories",
                "stars",
                "review_count",
                "actual_review_count",
                "actual_avg_stars"
            )
            .orderBy("business_id")
        )

        result_count = result.count()
        elapsed = time.time() - start_time
        print(f"✓ Completed in {elapsed:.2f}s - Found {result_count} businesses")
        return result



