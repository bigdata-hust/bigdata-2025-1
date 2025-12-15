"""
Analytics Functions - BATCH MODE
Optimized for batch processing without streaming overhead
"""
import time
from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast  # Explicit broadcast join
from pyspark.sql.window import Window


class YelpAnalytics:
    """Core analytics functions optimized for batch processing"""

    @staticmethod
    def top_selling_products_recent(review_df, business_df, days, top_n):
        """
        1. Top Selling Products (Last N days)
        Find businesses with most reviews in recent period
        """
        print(f"\n{'='*60}")
        print(f"Analysis 1: Top {top_n} Selling Products (Last {days} days)")
        print(f"{'='*60}")
        start_time = time.time()

        # Add salt to handle data skew
        review_with_salt = review_df.withColumn("salt", (rand() * 10).cast("int"))

        # Filter by date range (assuming date is in "yyyy-MM-dd HH:mm:ss" format)
        cutoff_date = date_sub(current_date(), days)
        review_with_salt = review_with_salt.withColumn(
            'date_parsed', to_timestamp(col('date'), 'yyyy-MM-dd HH:mm:ss')
        )
        recent_reviews = review_with_salt.filter(col("date_parsed") >= cutoff_date)

        # Stage 1: Salted aggregation to avoid skew
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
        top_candidates = business_stats.orderBy(desc("recent_review_count")).limit(top_n * 10)

        # Broadcast join with business info (business_df is small after select)
        result = top_candidates.join(
            broadcast(business_df.select("business_id", "name", "city", "state", "categories")),
            "business_id"
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "recent_review_count",
            round("avg_rating", 2).alias("avg_rating")
        ).orderBy(desc("recent_review_count")).limit(top_n)

        elapsed = time.time() - start_time
        print(f"✓ Analysis 1 completed in {elapsed:.2f}s")

        return result

    @staticmethod
    def top_stores_by_product_count(business_df, top_n=10):
        """
        2. Most Diverse Stores (by category count)
        Find stores with most product categories
        """
        print(f"\n{'='*60}")
        print(f"Analysis 2: Top {top_n} Stores by Product Diversity")
        print(f"{'='*60}")
        start_time = time.time()

        # Filter businesses with categories
        business_filtered = business_df \
            .filter(col("categories").isNotNull()) \
            .filter(length(col("categories")) > 0) \
            .select(
                "business_id", "name", "city", "state",
                "categories", "review_count", "stars"
            )

        # Count categories (split by comma)
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
        ).orderBy(desc("category_count")).limit(top_n)

        elapsed = time.time() - start_time
        print(f"✓ Analysis 2 completed in {elapsed:.2f}s")

        return result

    @staticmethod
    def top_rated_products(business_df, review_df, min_reviews=50, top_n=10):
        """
        3. Best Rated Products
        Find products with highest average rating (min reviews threshold)
        """
        print(f"\n{'='*60}")
        print(f"Analysis 3: Top {top_n} Rated Products (Min {min_reviews} reviews)")
        print(f"{'='*60}")
        start_time = time.time()

        # Aggregate review stats
        business_stats = review_df \
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
        top_candidates = qualified.orderBy(desc("avg_review_stars")).limit(top_n * 5)

        # Broadcast join with business info
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
            round("avg_review_stars", 2).alias("avg_review_stars"),
            "total_useful",
            col("stars").alias("business_avg_stars")
        ).orderBy(desc("avg_review_stars")).limit(top_n)

        elapsed = time.time() - start_time
        print(f"✓ Analysis 3 completed in {elapsed:.2f}s")

        return result

    @staticmethod
    def top_stores_by_positive_reviews(business_df, review_df,
                                       positive_threshold=4, top_n=10):
        """
        4. Stores with Most Positive Reviews
        Find stores with highest number of positive reviews
        """
        print(f"\n{'='*60}")
        print(f"Analysis 4: Top {top_n} Stores by Positive Reviews (>= {positive_threshold} stars)")
        print(f"{'='*60}")
        start_time = time.time()

        # Single-pass aggregation with conditional logic
        review_stats = review_df.groupBy("business_id").agg(
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
                round(col("positive_review_count") / col("total_review_count"), 3)
            ) \
            .filter(col("positive_review_count") > 0)

        # Get top candidates
        top_candidates = review_stats_filtered \
            .orderBy(desc("positive_review_count")) \
            .limit(top_n * 3)

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
            "positive_review_count",
            "total_review_count",
            "positive_ratio",
            round("avg_positive_rating", 2).alias("avg_positive_rating"),
            "total_useful_votes"
        ).orderBy(desc("positive_review_count")).limit(top_n)

        elapsed = time.time() - start_time
        print(f"✓ Analysis 4 completed in {elapsed:.2f}s")

        return result

    @staticmethod
    def get_peak_hours(review_df):
        """
        5. Peak Review Hours
        Analyze review activity over time (year, month, day)
        """
        print(f"\n{'='*60}")
        print("Analysis 5: Peak Review Hours (Activity Over Time)")
        print(f"{'='*60}")
        start_time = time.time()

        # Parse date (format: "yyyy-MM-dd HH:mm:ss")
        df = review_df.withColumn(
            "date_parsed",
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")
        )

        # Aggregate by year, month
        result = (
            df.groupBy(
                year("date_parsed").alias("year"),
                month("date_parsed").alias("month")
            )
            .agg(count("review_id").alias("review_count"))
            .orderBy("year", "month")
        )

        elapsed = time.time() - start_time
        print(f"✓ Analysis 5 completed in {elapsed:.2f}s")

        return result

    @staticmethod
    def get_top_categories(business_df, review_df, top_n=20):
        """
        6. Top Categories by Review Count
        Find most popular business categories
        """
        print(f"\n{'='*60}")
        print(f"Analysis 6: Top {top_n} Categories by Review Count")
        print(f"{'='*60}")
        start_time = time.time()

        # Split categories into separate rows
        df_business = business_df.withColumn(
            "category",
            explode(split(col("categories"), ",\\s*"))
        )

        # Broadcast join review with business (df_business after select is small)
        joined = review_df.join(
            broadcast(df_business.select("business_id", "category")),
            "business_id"
        )

        # Count reviews per category
        result = (
            joined.groupBy("category")
            .agg(count("review_id").alias("total_reviews"))
            .orderBy(desc("total_reviews"))
            .limit(top_n)
        )

        elapsed = time.time() - start_time
        print(f"✓ Analysis 6 completed in {elapsed:.2f}s")

        return result

    @staticmethod
    def get_store_stats(business_df, review_df):
        """
        7. Store Statistics Summary
        Comprehensive stats for all businesses
        """
        print(f"\n{'='*60}")
        print("Analysis 7: Store Statistics Summary")
        print(f"{'='*60}")
        start_time = time.time()

        # Calculate actual review stats
        review_stats = (
            review_df.groupBy("business_id")
            .agg(
                count("review_id").alias("actual_review_count"),
                avg("stars").alias("actual_avg_stars")
            )
        )

        # Join with business info
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
                round("actual_avg_stars", 2).alias("actual_avg_stars")
            )
            .orderBy(desc("actual_review_count"))
        )

        elapsed = time.time() - start_time
        print(f"✓ Analysis 7 completed in {elapsed:.2f}s")

        return result
