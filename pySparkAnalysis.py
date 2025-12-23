"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, desc, to_date, current_date, lit, 
    expr, rand, when, year, month, unix_timestamp, size, 
    split, trim, length, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
import time
from datetime import datetime


# ============================================================================
# CONFIGURATION & INITIALIZATION
# ============================================================================

class SparkConfig:
    """Spark configuration optimized for big data processing"""
    
    @staticmethod
    def create_spark_session():
        """
        Initialize Spark Session with optimized configurations
        """
        spark = SparkSession.builder \
            .appName("Yelp Big Data Analysis System") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "200") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
            .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.sql.files.openCostInBytes", "4194304") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setCheckpointDir("checkpoints/")
        
        return spark


# ============================================================================
# DATA SCHEMAS
# ============================================================================

class YelpSchemas:
    """Explicit schemas for better performance"""
    
    @staticmethod
    def business_schema():
        return StructType([
            StructField("business_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("stars", DoubleType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("is_open", IntegerType(), True),
            StructField("categories", StringType(), True),
        ])
    
    @staticmethod
    def review_schema():
        return StructType([
            StructField("review_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("stars", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("text", StringType(), True),
            StructField("useful", IntegerType(), True),
        ])
    
    @staticmethod
    def user_schema():
        return StructType([
            StructField("user_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("yelping_since", StringType(), True),
            StructField("useful", IntegerType(), True),
            StructField("fans", IntegerType(), True),
            StructField("average_stars", DoubleType(), True),
        ])


# ============================================================================
# DATA LOADING & PREPROCESSING
# ============================================================================

class DataLoader:
    """Handles data loading and preprocessing"""
    
    def __init__(self, spark, data_path="data/"):
        self.spark = spark
        self.data_path = data_path
        self.schemas = YelpSchemas()
    
    def load_business_data(self):
        """Load and prepare business data"""
        print("Loading business data...")
        
        business_df = self.spark.read \
            .schema(self.schemas.business_schema()) \
            .json(f"{self.data_path}business.json")
        
        # Repartition và cache
        business_df = business_df \
            .repartition(100, "business_id") \
            .cache()
        
        # Trigger cache
        count = business_df.count()
        print(f"Loaded {count:,} businesses")
        
        return business_df
    
    def load_review_data(self):
        """Load and prepare review data"""
        print("Loading review data...")
        
        review_df = self.spark.read \
            .schema(self.schemas.review_schema()) \
            .json(f"{self.data_path}review.json")
        
        # Preprocess dates
        review_df = review_df \
            .withColumn("review_date", to_date(col("date"))) \
            .withColumn("review_timestamp", unix_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("review_year", year(col("date"))) \
            .withColumn("review_month", month(col("date")))
        
        # Repartition by business_id for joins
        review_df = review_df.repartition(200, "business_id")
        
        count = review_df.count()
        print(f"Loaded {count:,} reviews")
        
        return review_df
    
    def load_user_data(self):
        """Load user data"""
        print("Loading user data...")
        
        user_df = self.spark.read \
            .schema(self.schemas.user_schema()) \
            .json(f"{self.data_path}user.json")
        
        count = user_df.count()
        print(f"Loaded {count:,} users")
        
        return user_df


# ============================================================================
# ANALYSIS FUNCTIONS - OPTIMIZED FOR BIG DATA
# ============================================================================

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
        cutoff_date = current_date() - lit(days)
        recent_reviews = review_with_salt.filter(col("review_date") >= cutoff_date)
        
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


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================

class YelpAnalysisPipeline:
    """
    Main pipeline orchestrator
    Production-ready with error handling, monitoring, and checkpointing
    """
    
    def __init__(self, data_path="data/", output_path="output/"):
        self.data_path = data_path
        self.output_path = output_path
        self.spark = SparkConfig.create_spark_session()
        self.data_loader = DataLoader(self.spark, data_path)
        self.analytics = YelpAnalytics()
        self.results = {}
    
    def load_data(self):
        """Load all datasets"""
        print("\n" + "="*60)
        print("DATA LOADING PHASE")
        print("="*60)
        
        self.business_df = self.data_loader.load_business_data()
        self.review_df = self.data_loader.load_review_data()
        # self.user_df = self.data_loader.load_user_data()  # Load if needed
        
        # Checkpoint business data (reused multiple times)
        self.business_df.checkpoint()
        
        print("\n✓ All data loaded successfully")
    
    def run_analysis_1(self, days=90, top_n=10):
        """Run Analysis 1: Top Selling Products"""
        try:
            result = self.analytics.top_selling_products_recent(
                self.review_df, self.business_df, days=days, top_n=top_n
            )
            self.results['top_selling'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 1: {str(e)}")
            raise
    
    def run_analysis_2(self, top_n=10):
        """Run Analysis 2: Top Diverse Stores"""
        try:
            result = self.analytics.top_stores_by_product_count(
                self.business_df, top_n=top_n
            )
            self.results['diverse_stores'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 2: {str(e)}")
            raise
    
    def run_analysis_3(self, min_reviews=50, top_n=10):
        """Run Analysis 3: Top Rated Products"""
        try:
            result = self.analytics.top_rated_products(
                self.business_df, self.review_df, 
                min_reviews=min_reviews, top_n=top_n
            )
            self.results['best_rated'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 3: {str(e)}")
            raise
    
    def run_analysis_4(self, positive_threshold=4, top_n=10):
        """Run Analysis 4: Top Stores by Positive Reviews"""
        try:
            result = self.analytics.top_stores_by_positive_reviews(
                self.business_df, self.review_df,
                positive_threshold=positive_threshold, top_n=top_n
            )
            self.results['most_positive'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 4: {str(e)}")
            raise
    
    def run_all_analyses(self, config=None):
        """
        Run all analyses with custom configuration
        
        Args:
            config: dict with parameters for each analysis
        """
        if config is None:
            config = {
                'analysis_1': {'days': 90, 'top_n': 10},
                'analysis_2': {'top_n': 10},
                'analysis_3': {'min_reviews': 50, 'top_n': 10},
                'analysis_4': {'positive_threshold': 4, 'top_n': 10}
            }
        
        print("\n" + "="*60)
        print("ANALYSIS PHASE - RUNNING ALL ANALYSES")
        print("="*60)
        
        total_start = time.time()
        
        # Run all analyses
        self.run_analysis_1(**config['analysis_1'])
        self.run_analysis_2(**config['analysis_2'])
        self.run_analysis_3(**config['analysis_3'])
        self.run_analysis_4(**config['analysis_4'])
        
        total_elapsed = time.time() - total_start
        
        print("\n" + "="*60)
        print(f"ALL ANALYSES COMPLETED in {total_elapsed:.2f}s")
        print("="*60)
    
    def display_results(self):
        """Display all results"""
        print("\n" + "="*60)
        print("RESULTS PREVIEW")
        print("="*60)
        
        for name, df in self.results.items():
            print(f"\n{name.upper().replace('_', ' ')}:")
            print("-" * 60)
            df.show(truncate=False)
    
    def save_results(self, format='parquet', coalesce=True):
        """
        Save results to disk
        
        Args:
            format: output format ('parquet', 'csv', 'json')
            coalesce: whether to coalesce to single file
        """
        print("\n" + "="*60)
        print("SAVING RESULTS")
        print("="*60)
        
        for name, df in self.results.items():
            output_path = f"{self.output_path}{name}"
            
            try:
                writer = df.coalesce(1) if coalesce else df
                
                if format == 'parquet':
                    writer.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .parquet(output_path)
                elif format == 'csv':
                    writer.write \
                        .mode("overwrite") \
                        .option("header", "true") \
                        .csv(output_path)
                elif format == 'json':
                    writer.write \
                        .mode("overwrite") \
                        .json(output_path)
                
                print(f"✓ Saved {name} to {output_path}")
            
            except Exception as e:
                print(f"✗ Error saving {name}: {str(e)}")
    
    def generate_summary_report(self):
        """Generate summary statistics"""
        print("\n" + "="*60)
        print("SUMMARY REPORT")
        print("="*60)
        
        print(f"\nTotal Businesses: {self.business_df.count():,}")
        print(f"Total Reviews: {self.review_df.count():,}")
        
        for name, df in self.results.items():
            print(f"\n{name.upper().replace('_', ' ')}:")
            print(f"  Results: {df.count()}")
    
    def cleanup(self):
        """Cleanup resources"""
        print("\n" + "="*60)
        print("CLEANUP")
        print("="*60)
        
        # Unpersist cached data
        self.business_df.unpersist()
        
        print("✓ Resources cleaned up")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("✓ Spark session stopped")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function
    """
    print("\n" + "="*80)
    print(" " * 20 + "YELP BIG DATA ANALYSIS SYSTEM")
    print(" " * 25 + "Optimized for Large-Scale Processing")
    print("="*80)
    
    # Initialize pipeline
    pipeline = YelpAnalysisPipeline(
        data_path="data/",
        output_path="output/"
    )
    
    try:
        # Step 1: Load data
        pipeline.load_data()
        
        # Step 2: Run all analyses
        pipeline.run_all_analyses(config={
            'analysis_1': {'days': 90, 'top_n': 10},
            'analysis_2': {'top_n': 10},
            'analysis_3': {'min_reviews': 50, 'top_n': 10},
            'analysis_4': {'positive_threshold': 4, 'top_n': 10}
        })
        
        # Step 3: Display results
        pipeline.display_results()
        
        # Step 4: Save results
        pipeline.save_results(format='parquet', coalesce=True)
        
        # Step 5: Generate summary
        pipeline.generate_summary_report()
        
        # Step 6: Cleanup
        pipeline.cleanup()
        
        print("\n" + "="*80)
        print(" " * 25 + "PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80 + "\n")
    
    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always stop Spark
        pipeline.stop()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def run_single_analysis(analysis_number, **kwargs):
    """
    Run a single analysis independently
    
    Args:
        analysis_number: 1, 2, 3, or 4
        **kwargs: parameters for the specific analysis
    """
    pipeline = YelpAnalysisPipeline()
    
    try:
        pipeline.load_data()
        
        if analysis_number == 1:
            result = pipeline.run_analysis_1(**kwargs)
        elif analysis_number == 2:
            result = pipeline.run_analysis_2(**kwargs)
        elif analysis_number == 3:
            result = pipeline.run_analysis_3(**kwargs)
        elif analysis_number == 4:
            result = pipeline.run_analysis_4(**kwargs)
        else:
            raise ValueError("Analysis number must be 1, 2, 3, or 4")
        
        result.show(truncate=False)
        return result
    
    finally:
        pipeline.stop()


def run_custom_analysis(business_df, review_df, analysis_func, **kwargs):
    """
    Run custom analysis function
    
    Args:
        business_df: business DataFrame
        review_df: review DataFrame
        analysis_func: custom analysis function
        **kwargs: parameters for the analysis function
    """
    return analysis_func(business_df, review_df, **kwargs)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Run full pipeline
    main()
    
    # Or run single analysis:
    # run_single_analysis(1, days=90, top_n=10)
    # run_single_analysis(2, top_n=15)
    # run_single_analysis(3, min_reviews=100, top_n=20)
    # run_single_analysis(4, positive_threshold=4, top_n=10)