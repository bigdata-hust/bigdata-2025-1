"""
Pipeline Orchestration - BATCH MODE
Manages execution of all analytics in batch processing mode
"""
import os
import time
from datetime import datetime

from batch_analytics import YelpAnalytics
from batch_load_data import DataLoader
from batch_configuration import SparkConfig


class YelpAnalysisPipeline:
    """
    Main pipeline orchestrator for batch processing
    Handles data loading, analysis execution, and result output
    """

    def __init__(self, data_path=None, output_path=None):
        self.data_path = data_path
        self.output_path = output_path or "./output/"
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
        # self.user_df = self.data_loader.load_user_data()  # Optional

        print("\n✓ All data loaded successfully")

    def run_analysis_1(self, days=15, top_n=10):
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
            result = self.analytics.get_top_categories(
                self.business_df, self.review_df, top_n=top_n
            )
            self.results['top_categories'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 6: {str(e)}")
            raise

    def run_analysis_7(self):
        """Run Analysis 7: Overall Store Statistics Summary"""
        try:
            result = self.analytics.get_store_stats(
                self.business_df, self.review_df
            )
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
                'analysis_1': {'days': 15, 'top_n': 10},
                'analysis_2': {'top_n': 10},
                'analysis_3': {'min_reviews': 10, 'top_n': 10},
                'analysis_4': {'positive_threshold': 4, 'top_n': 10},
                'analysis_6': {'top_n': 20}
            }

        print("\n" + "="*60)
        print("ANALYSIS PHASE - RUNNING ALL ANALYSES")
        print("="*60)

        total_start = time.time()

        # Run all 7 analyses
        self.run_analysis_1(**config.get('analysis_1', {}))
        self.run_analysis_2(**config.get('analysis_2', {}))
        self.run_analysis_3(**config.get('analysis_3', {}))
        self.run_analysis_4(**config.get('analysis_4', {}))
        self.run_analysis_5()
        self.run_analysis_6(**config.get('analysis_6', {}))
        self.run_analysis_7()

        total_elapsed = time.time() - total_start
        print("\n" + "="*60)
        print(f"ALL ANALYSES COMPLETED in {total_elapsed:.2f}s")
        print("="*60)

    def display_results(self, show_rows=10):
        """Display all results on console"""
        print("\n" + "="*80)
        print(" " * 30 + "RESULTS PREVIEW")
        print("="*80)

        for name, df in self.results.items():
            print(f"\n{' ' + name.upper().replace('_', ' ') + ' ':=^80}")
            df.show(show_rows, truncate=False)

    def save_results(self, format='parquet'):
        """
        Save results to disk

        Args:
            format: output format ('parquet', 'csv', 'json')
        """
        print("\n" + "="*60)
        print("SAVING RESULTS TO DISK")
        print("="*60)

        # Create output directory if not exists
        os.makedirs(self.output_path, exist_ok=True)

        for name, df in self.results.items():
            output_path = os.path.join(self.output_path, name)

            try:
                if format == 'parquet':
                    df.write.mode('overwrite').parquet(output_path)
                elif format == 'csv':
                    df.write.mode('overwrite').option('header', 'true').csv(output_path)
                elif format == 'json':
                    df.write.mode('overwrite').json(output_path)
                else:
                    raise ValueError(f"Unsupported format: {format}")

                print(f"✓ Saved {name} to {output_path}/ ({format})")

            except Exception as e:
                print(f"✗ Error saving {name}: {str(e)}")

        print(f"\n✓ All results saved to: {self.output_path}")

    def generate_summary_report(self):
        """Generate summary statistics"""
        print("\n" + "="*80)
        print(" " * 30 + "SUMMARY REPORT")
        print("="*80)

        for name, df in self.results.items():
            count = df.count()
            print(f"\n{name.upper().replace('_', ' ')}:")
            print(f"  - Total records: {count:,}")

    def cleanup(self):
        """Cleanup resources"""
        print("\n" + "="*60)
        print("CLEANUP")
        print("="*60)

        # Unpersist cached data
        if hasattr(self, 'business_df'):
            self.business_df.unpersist()
        if hasattr(self, 'review_df'):
            self.review_df.unpersist()

        print("✓ Resources cleaned up")

    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("✓ Spark session stopped")
