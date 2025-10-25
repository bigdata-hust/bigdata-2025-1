# ============================================================================
# PIPELINE ORCHESTRATION
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

from analytics_yelp import YelpAnalytics
from load_data import DataLoader
from configuration import SparkConfig , YelpSchemas


from configuration import SparkConfig
class YelpAnalysisPipeline:
    """
    Main pipeline orchestrator
    Production-ready with error handling, monitoring, and checkpointing
    """
    
    def __init__(self, data_path="../data/", output_path="output/"):
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
    
    def run_analysis_5(self):
        """Run Analysis 5: Review Activity Over Time (Peak Hours)"""
        try:
            result = self.analytics.get_peak_hours(self.review_df)
            self.results['peak_hours'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 5: {str(e)}")
            raise

   
    def run_analysis_6(self, top_n=20):
        """Run Analysis 6: Top Business Categories by Review Count"""
        try:
            result = self.analytics.get_top_categories(self.business_df, self.review_df, top_n=top_n)
            self.results['top_categories'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 6: {str(e)}")
            raise

    
    def run_analysis_7(self):
        """Run Analysis 7: Overall Store Statistics Summary"""
        try:
            result = self.analytics.get_store_stats(self.business_df, self.review_df)
            self.results['store_stats'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 7: {str(e)}")
            raise

    
    def run_all_analyses(self, config=None):
        """
        Run all analyses with custom configuration
        """
        if config is None:
            config = {
                'analysis_1': {'days': 90, 'top_n': 10},
                'analysis_2': {'top_n': 10},
                'analysis_3': {'min_reviews': 10, 'top_n': 10},
                'analysis_4': {'positive_threshold': 4, 'top_n': 10},
                'analysis_6': {'top_n': 20}
            }

        print("\n" + "="*60)
        print("ANALYSIS PHASE - RUNNING ALL ANALYSES")
        print("="*60)

        total_start = time.time()

        
        self.run_analysis_1(**config['analysis_1'])
        self.run_analysis_2(**config['analysis_2'])
        self.run_analysis_3(**config['analysis_3'])
        self.run_analysis_4(**config['analysis_4'])
        self.run_analysis_5()
        self.run_analysis_6(**config['analysis_6'])
        self.run_analysis_7()

        print("=== BEFORE ANALYSIS 3 ===")
        print("Business rows:", self.business_df.count())
        print("Review rows:", self.review_df.count())

        result3 = self.run_analysis_3(**config.get("analysis_3", {}))
        print("Analysis 3 result rows:", result3.count())
        result3.show(5, truncate=False)

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


