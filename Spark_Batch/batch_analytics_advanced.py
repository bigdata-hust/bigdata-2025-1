"""
Advanced Analytics Functions - Phương án 1
Demonstrates advanced Spark skills:
- Window Functions (lag, lead, rank, moving averages)
- Pivot/Unpivot operations
- Complex aggregations
"""
import time
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *


class AdvancedYelpAnalytics:
    """Advanced analytics using Window Functions and Pivot operations"""

    @staticmethod
    def trending_businesses(review_df, business_df, window_days=90, top_n=10):
        """
        Analysis 8: Trending Businesses (Window Functions)

        Find businesses with increasing review trends using window functions.
        Demonstrates: lag(), lead(), avg() over window, dense_rank(), rowsBetween()

        Args:
            review_df: DataFrame with review data
            business_df: DataFrame with business data
            window_days: Number of days to analyze (default: 90)
            top_n: Number of top trending businesses to return

        Returns:
            DataFrame with trending businesses ranked by growth rate

        Window Functions used:
            - lag(): Compare with previous week
            - avg() over window: Moving average (4 weeks)
            - dense_rank(): Rank by growth rate
            - rowsBetween(): Define window frame
        """
        print(f"\n{'='*60}")
        print(f"Analysis 8: Top {top_n} Trending Businesses (Last {window_days} days)")
        print(f"{'='*60}")
        start_time = time.time()

        # Step 1: Parse review dates
        df = review_df.withColumn(
            "review_date",
            to_date(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        )

        # Filter recent reviews
        cutoff_date = date_sub(current_date(), window_days)
        recent_df = df.filter(col("review_date") >= cutoff_date)

        print(f"  - Analyzing reviews from {window_days} days")

        # Step 2: Group by business and week (7-day window)
        weekly_reviews = recent_df.groupBy(
            "business_id",
            window("review_date", "7 days").alias("week")
        ).agg(
            count("review_id").alias("weekly_count"),
            avg("stars").alias("weekly_avg_stars")
        ).select(
            "business_id",
            col("week.start").alias("week_start"),
            col("week.end").alias("week_end"),
            "weekly_count",
            "weekly_avg_stars"
        )

        print(f"  - Grouped into weekly buckets")

        # Step 3: Define window specification
        # Partition by business_id, order by week_start
        windowSpec = Window.partitionBy("business_id").orderBy("week_start")

        # Step 4: Apply window functions
        trending = weekly_reviews.withColumn(
            # Previous week count (lag)
            "prev_week_count",
            lag("weekly_count", 1).over(windowSpec)
        ).withColumn(
            # Next week count (lead)
            "next_week_count",
            lead("weekly_count", 1).over(windowSpec)
        ).withColumn(
            # Growth rate vs previous week
            "growth_rate",
            when(col("prev_week_count") > 0,
                 (col("weekly_count") - col("prev_week_count")) / col("prev_week_count")
            ).otherwise(0.0)
        ).withColumn(
            # Moving average (last 4 weeks including current)
            "avg_last_4_weeks",
            avg("weekly_count").over(
                windowSpec.rowsBetween(-3, 0)
            )
        ).withColumn(
            # Cumulative sum (all weeks up to current)
            "cumulative_reviews",
            sum("weekly_count").over(
                windowSpec.rowsBetween(Window.unboundedPreceding, 0)
            )
        ).withColumn(
            # Week number (row number within business)
            "week_number",
            row_number().over(windowSpec)
        )

        print(f"  - Applied window functions: lag, lead, avg, sum, row_number")

        # Step 5: Filter for recent weeks with high growth
        recent_trending = trending.filter(
            (col("week_start") >= date_sub(current_date(), 30))  # Last 30 days
            & (col("growth_rate") > 0.1)  # At least 10% growth
            & (col("prev_week_count") > 0)  # Must have previous data
        )

        # Step 6: Rank businesses by growth rate
        windowRank = Window.orderBy(desc("growth_rate"))

        ranked = recent_trending.withColumn(
            "trend_rank",
            dense_rank().over(windowRank)
        )

        print(f"  - Ranked businesses by growth rate")

        # Step 7: Get top trending businesses
        top_trending = ranked.filter(col("trend_rank") <= top_n)

        # Step 8: Join with business info
        result = top_trending.join(
            business_df.select("business_id", "name", "city", "state", "categories"),
            "business_id"
        ).select(
            "business_id",
            "name",
            "city",
            "state",
            "categories",
            "week_start",
            "weekly_count",
            "prev_week_count",
            round("growth_rate", 3).alias("growth_rate"),
            round("avg_last_4_weeks", 1).alias("avg_last_4_weeks"),
            "cumulative_reviews",
            "trend_rank"
        ).orderBy("trend_rank")

        elapsed = time.time() - start_time
        print(f"✓ Analysis 8 completed in {elapsed:.2f}s")
        print(f"  Found {result.count()} trending businesses")

        return result

    @staticmethod
    def category_performance_matrix(business_df, review_df, top_categories=10, top_cities=5):
        """
        Analysis 9: Category Performance Matrix (Pivot/Unpivot)

        Create a performance matrix showing categories vs cities.
        Demonstrates: pivot(), unpivot (via stack), explode()

        Args:
            business_df: DataFrame with business data
            review_df: DataFrame with review data
            top_categories: Number of top categories to include
            top_cities: Number of top cities to include

        Returns:
            dict with:
                'pivot': Wide format (categories as rows, cities as columns)
                'unpivot': Long format (category, city, metrics)
                'summary': Summary statistics

        Pivot/Unpivot operations:
            - explode(): Split categories into rows
            - pivot(): Transform long → wide format
            - stack(): Transform wide → long format (unpivot)
        """
        print(f"\n{'='*60}")
        print(f"Analysis 9: Category Performance Matrix (Pivot/Unpivot)")
        print(f"{'='*60}")
        start_time = time.time()

        # Step 1: Get top categories and cities
        print(f"  - Finding top {top_categories} categories and {top_cities} cities...")

        # Explode categories
        business_with_cat = business_df.withColumn(
            "category",
            explode(split(trim(col("categories")), "\\s*,\\s*"))
        ).filter(
            col("category").isNotNull() & (length(col("category")) > 0)
        )

        # Get top categories by business count
        top_cat_list = business_with_cat.groupBy("category") \
            .agg(count("business_id").alias("business_count")) \
            .orderBy(desc("business_count")) \
            .limit(top_categories) \
            .select("category") \
            .rdd.flatMap(lambda x: x).collect()

        # Get top cities by business count
        top_city_list = business_df.groupBy("city") \
            .agg(count("business_id").alias("business_count")) \
            .orderBy(desc("business_count")) \
            .limit(top_cities) \
            .select("city") \
            .rdd.flatMap(lambda x: x).collect()

        print(f"  - Top categories: {', '.join(top_cat_list[:3])}...")
        print(f"  - Top cities: {', '.join(top_city_list[:3])}...")

        # Step 2: Filter to top categories and cities
        filtered_business = business_with_cat.filter(
            col("category").isin(top_cat_list) &
            col("city").isin(top_city_list)
        )

        # Step 3: Join with reviews and aggregate
        joined = filtered_business.join(review_df, "business_id")

        agg_df = joined.groupBy("category", "city").agg(
            avg("stars").alias("avg_stars"),
            count("review_id").alias("review_count"),
            countDistinct("business_id").alias("business_count")
        )

        print(f"  - Aggregated metrics by category and city")

        # Step 4: PIVOT - Create wide format (categories as rows, cities as columns)
        print(f"  - Creating pivot table...")

        # Create composite metric column
        pivoted = agg_df.groupBy("category").pivot("city").agg(
            first(
                concat(
                    round("avg_stars", 1).cast("string"),
                    lit(" ("),
                    col("review_count").cast("string"),
                    lit(")")
                )
            )
        ).orderBy("category")

        # Fill nulls
        for city in top_city_list:
            if city in pivoted.columns:
                pivoted = pivoted.withColumn(
                    city,
                    when(col(city).isNull(), lit("N/A")).otherwise(col(city))
                )

        print(f"  - Pivot complete: {len(pivoted.columns)-1} cities, {pivoted.count()} categories")

        # Step 5: UNPIVOT - Convert back to long format
        print(f"  - Creating unpivot table...")

        # Method 1: Using stack (more efficient)
        city_exprs = ", ".join([f"'{city}', `{city}`" for city in top_city_list if city in pivoted.columns])

        unpivoted = pivoted.select(
            "category",
            expr(f"stack({len([c for c in top_city_list if c in pivoted.columns])}, {city_exprs}) as (city, metrics)")
        ).filter(
            col("metrics").isNotNull() & (col("metrics") != "N/A")
        )

        # Parse metrics back
        unpivoted = unpivoted.withColumn(
            "avg_stars_str",
            split(col("metrics"), " \\(")[0]
        ).withColumn(
            "review_count_str",
            regexp_extract(col("metrics"), "\\((\\d+)\\)", 1)
        ).withColumn(
            "avg_stars",
            col("avg_stars_str").cast("float")
        ).withColumn(
            "review_count",
            col("review_count_str").cast("int")
        ).select(
            "category",
            "city",
            "avg_stars",
            "review_count"
        ).orderBy("category", "city")

        print(f"  - Unpivot complete: {unpivoted.count()} rows")

        # Step 6: Create summary statistics
        summary = agg_df.groupBy().agg(
            countDistinct("category").alias("total_categories"),
            countDistinct("city").alias("total_cities"),
            avg("avg_stars").alias("overall_avg_stars"),
            sum("review_count").alias("total_reviews"),
            sum("business_count").alias("total_businesses")
        )

        # Additional: Best category per city
        windowSpec = Window.partitionBy("city").orderBy(desc("avg_stars"))
        best_per_city = agg_df.withColumn(
            "rank",
            row_number().over(windowSpec)
        ).filter(col("rank") == 1).select(
            "city",
            col("category").alias("best_category"),
            round("avg_stars", 2).alias("avg_stars"),
            "review_count"
        ).orderBy("city")

        # Additional: Best city per category
        windowSpec2 = Window.partitionBy("category").orderBy(desc("avg_stars"))
        best_per_category = agg_df.withColumn(
            "rank",
            row_number().over(windowSpec2)
        ).filter(col("rank") == 1).select(
            "category",
            col("city").alias("best_city"),
            round("avg_stars", 2).alias("avg_stars"),
            "review_count"
        ).orderBy("category")

        elapsed = time.time() - start_time
        print(f"✓ Analysis 9 completed in {elapsed:.2f}s")

        return {
            'pivot': pivoted,
            'unpivot': unpivoted,
            'summary': summary,
            'best_per_city': best_per_city,
            'best_per_category': best_per_category
        }

    @staticmethod
    def user_engagement_analysis(review_df, user_df=None, lookback_days=180):
        """
        Analysis 10 (Bonus): User Engagement Patterns (Window Functions)

        Analyze user review patterns over time.
        Demonstrates: Multiple window specifications, complex aggregations

        Args:
            review_df: DataFrame with review data
            user_df: DataFrame with user data (optional)
            lookback_days: Days to look back (default: 180)

        Returns:
            DataFrame with user engagement metrics
        """
        print(f"\n{'='*60}")
        print(f"Analysis 10: User Engagement Patterns (Last {lookback_days} days)")
        print(f"{'='*60}")
        start_time = time.time()

        # Parse dates
        df = review_df.withColumn(
            "review_date",
            to_date(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        )

        # Filter recent
        cutoff_date = date_sub(current_date(), lookback_days)
        recent = df.filter(col("review_date") >= cutoff_date)

        # Window by user, ordered by date
        userWindow = Window.partitionBy("user_id").orderBy("review_date")

        # Calculate engagement metrics
        engagement = recent.withColumn(
            # Days since last review
            "days_since_last_review",
            datediff(col("review_date"), lag("review_date", 1).over(userWindow))
        ).withColumn(
            # Review frequency (reviews per week)
            "review_frequency",
            count("review_id").over(
                userWindow.rowsBetween(Window.unboundedPreceding, 0)
            ) / (datediff(col("review_date"),
                         min("review_date").over(userWindow)) / 7.0 + 1)
        ).withColumn(
            # Average rating trend
            "avg_rating_last_5",
            avg("stars").over(userWindow.rowsBetween(-4, 0))
        ).withColumn(
            # Review streak (consecutive days with reviews)
            "is_consecutive_day",
            when(col("days_since_last_review") <= 1, 1).otherwise(0)
        )

        # Aggregate per user
        user_summary = engagement.groupBy("user_id").agg(
            count("review_id").alias("total_reviews"),
            avg("stars").alias("avg_rating"),
            avg("days_since_last_review").alias("avg_days_between_reviews"),
            max("review_frequency").alias("peak_frequency"),
            sum("is_consecutive_day").alias("consecutive_review_days")
        )

        # Categorize users
        result = user_summary.withColumn(
            "engagement_level",
            when(col("total_reviews") >= 20, "Power User")
            .when(col("total_reviews") >= 10, "Active User")
            .when(col("total_reviews") >= 5, "Regular User")
            .otherwise("Casual User")
        ).orderBy(desc("total_reviews"))

        elapsed = time.time() - start_time
        print(f"✓ Analysis 10 completed in {elapsed:.2f}s")

        return result


if __name__ == "__main__":
    """
    Test script - run this file to test advanced analytics

    Usage:
        python3 batch_analytics_advanced.py
    """
    from pyspark.sql import SparkSession
    from batch_configuration import SparkConfig
    from batch_load_data import DataLoader

    print("="*60)
    print("ADVANCED ANALYTICS - TEST")
    print("="*60)

    # Create Spark session
    spark = SparkConfig.create_spark_session()

    # Try to load real data if exists
    try:
        loader = DataLoader(spark, "../data/")
        business_df = loader.load_business_data()
        review_df = loader.load_review_data()

        print("\n✓ Loaded real data\n")

        # Test Analysis 8
        analytics = AdvancedYelpAnalytics()

        trending = analytics.trending_businesses(review_df, business_df, window_days=90, top_n=10)
        print("\nTrending Businesses (Top 5):")
        trending.show(5, truncate=False)

        # Test Analysis 9
        matrix_results = analytics.category_performance_matrix(business_df, review_df, top_categories=5, top_cities=3)

        print("\nCategory Performance Matrix (Pivot):")
        matrix_results['pivot'].show(truncate=False)

        print("\nCategory Performance Matrix (Unpivot - sample):")
        matrix_results['unpivot'].show(10, truncate=False)

        print("\nBest Category per City:")
        matrix_results['best_per_city'].show(truncate=False)

        print("\n" + "="*60)
        print("✓ All advanced analytics working!")
        print("="*60)

    except Exception as e:
        print(f"\n⚠ Could not load data: {e}")
        print("Run with sample data instead:")
        print("  cd ../data && python3 ../Spark_Batch/create_sample_data.py")

    spark.stop()
