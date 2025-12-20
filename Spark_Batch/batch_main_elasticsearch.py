#!/usr/bin/env python3
"""
Yelp Big Data Analysis System with Elasticsearch Integration
Main Entry Point for Batch Processing + Kibana Visualization

Usage:
    python batch_main_elasticsearch.py [--data-path DATA_PATH] [--es-host ES_HOST] [--es-port ES_PORT]

Examples:
    # Local mode
    python batch_main_elasticsearch.py --data-path ./data/

    # Docker mode
    python batch_main_elasticsearch.py --data-path ./data/ --es-host elasticsearch --es-port 9200

    # With custom Elasticsearch
    python batch_main_elasticsearch.py --data-path ./data/ --es-host localhost --es-port 9200
"""

import sys
import argparse
import traceback
from datetime import datetime

# Import from existing modules
from batch_pipeline import YelpAnalysisPipeline
from batch_analytics_advanced import AdvancedYelpAnalytics
from batch_udf import *  # Import all UDFs
from save_elasticsearch import ElasticsearchSaver, initialize_all_indices


class EnhancedYelpPipelineWithES(YelpAnalysisPipeline):
    """
    Enhanced pipeline with Elasticsearch integration
    Extends YelpAnalysisPipeline with ES saving capabilities
    """

    def __init__(self, data_path=None, output_path=None, es_host="localhost", es_port=9200):
        """
        Initialize pipeline with Elasticsearch support

        Args:
            data_path: Path to input data
            output_path: Path to save CSV outputs
            es_host: Elasticsearch hostname
            es_port: Elasticsearch port
        """
        super().__init__(data_path, output_path)

        # Initialize Elasticsearch saver
        self.es_saver = ElasticsearchSaver(es_host, es_port)
        self.advanced_analytics = AdvancedYelpAnalytics()

        # Test Elasticsearch connection
        if not self.es_saver.test_connection():
            print("⚠ Warning: Elasticsearch connection failed. Results will only be saved to CSV.")
            self.es_enabled = False
        else:
            print("✓ Elasticsearch connected successfully")
            self.es_enabled = True

    def run_analysis_8(self, window_days=90, top_n=10):
        """Run Analysis 8: Trending Businesses (Window Functions)"""
        try:
            print(f"\n{'='*60}")
            print("ANALYSIS 8: Trending Businesses")
            print(f"{'='*60}")

            result = self.advanced_analytics.trending_businesses(
                self.review_df, self.business_df,
                window_days=window_days, top_n=top_n
            )

            self.results['trending_businesses'] = result

            # Save to Elasticsearch
            if self.es_enabled:
                self.es_saver.save_dataframe(
                    result,
                    "yelp-analysis-8-trending",
                    mode="overwrite"
                )

            print(f"✓ Analysis 8 completed")
            return result

        except Exception as e:
            print(f"✗ Error in Analysis 8: {str(e)}")
            traceback.print_exc()
            raise

    def run_analysis_9(self, top_categories=10, top_cities=5):
        """Run Analysis 9: Category Performance Matrix (Pivot/Unpivot)"""
        try:
            print(f"\n{'='*60}")
            print("ANALYSIS 9: Category Performance Matrix")
            print(f"{'='*60}")

            result = self.advanced_analytics.category_performance_matrix(
                self.business_df, self.review_df,
                top_categories=top_categories, top_cities=top_cities
            )

            self.results['performance_matrix'] = result

            # Save to Elasticsearch (unpivoted version is better for Kibana)
            if self.es_enabled:
                self.es_saver.save_dataframe(
                    result,
                    "yelp-analysis-9-performance-matrix",
                    mode="overwrite"
                )

            print(f"✓ Analysis 9 completed")
            return result

        except Exception as e:
            print(f"✗ Error in Analysis 9: {str(e)}")
            traceback.print_exc()
            raise

    def save_analysis_to_es(self, analysis_name, df, index_name):
        """
        Save analysis result to Elasticsearch

        Args:
            analysis_name: Name of the analysis
            df: DataFrame with results
            index_name: Elasticsearch index name
        """
        if not self.es_enabled:
            print(f"ℹ Elasticsearch disabled, skipping save for {analysis_name}")
            return False

        try:
            print(f"\nSaving {analysis_name} to Elasticsearch...")
            self.es_saver.save_dataframe(df, index_name, mode="overwrite")
            return True
        except Exception as e:
            print(f"✗ Error saving {analysis_name} to ES: {str(e)}")
            return False

    def run_all_analyses_with_es(self, config=None):
        """
        Run all 9 analyses and save to both CSV and Elasticsearch

        Args:
            config: Configuration dictionary for analyses
        """
        if config is None:
            config = {
                'analysis_1': {'days': 15, 'top_n': 10},
                'analysis_2': {'min_reviews': 10, 'top_n': 10},
                'analysis_3': {'min_reviews': 50, 'top_n': 10},
                'analysis_4': {},
                'analysis_5': {'min_stars': 4.0, 'max_reviews': 50, 'top_n': 10},
                'analysis_6': {'top_n': 10},
                'analysis_7': {},
                'analysis_8': {'window_days': 90, 'top_n': 10},
                'analysis_9': {'top_categories': 10, 'top_cities': 5}
            }

        print("\n" + "="*80)
        print(" " * 15 + "RUNNING ALL ANALYSES WITH ELASTICSEARCH")
        print("="*80)

        try:
            # Analysis 1: Top Selling Products
            print("\n[1/9] Running Analysis 1: Top Selling Products...")
            result1 = self.run_analysis_1(**config['analysis_1'])
            self.save_analysis_to_es("Top Selling", result1, "yelp-analysis-1-top-selling")

            # Analysis 2: User Purchase Patterns
            print("\n[2/9] Running Analysis 2: User Purchase Patterns...")
            result2 = self.run_analysis_2(**config['analysis_2'])
            self.save_analysis_to_es("User Patterns", result2, "yelp-analysis-2-user-patterns")

            # Analysis 3: Top Users by Reviews
            print("\n[3/9] Running Analysis 3: Top Users by Reviews...")
            result3 = self.run_analysis_3(**config['analysis_3'])
            self.save_analysis_to_es("Top Users", result3, "yelp-analysis-3-top-users")

            # Analysis 4: Category Trends
            print("\n[4/9] Running Analysis 4: Category Trends Over Time...")
            result4 = self.run_analysis_4(**config['analysis_4'])
            self.save_analysis_to_es("Category Trends", result4, "yelp-analysis-4-category-trends")

            # Analysis 5: High Rating Low Review
            print("\n[5/9] Running Analysis 5: High Rating Low Review Count...")
            result5 = self.run_analysis_5(**config['analysis_5'])
            self.save_analysis_to_es("High Rating Low Review", result5, "yelp-analysis-5-high-rating-low-review")

            # Analysis 6: Geographic Distribution
            print("\n[6/9] Running Analysis 6: Geographic Distribution...")
            result6 = self.run_analysis_6(**config['analysis_6'])
            self.save_analysis_to_es("Geographic", result6, "yelp-analysis-6-geographic")

            # Analysis 7: Seasonal Trends
            print("\n[7/9] Running Analysis 7: Seasonal Trends...")
            result7 = self.run_analysis_7(**config['analysis_7'])
            self.save_analysis_to_es("Seasonal Trends", result7, "yelp-analysis-7-seasonal")

            # Analysis 8: Trending Businesses (Window Functions)
            print("\n[8/9] Running Analysis 8: Trending Businesses...")
            result8 = self.run_analysis_8(**config['analysis_8'])

            # Analysis 9: Performance Matrix (Pivot/Unpivot)
            print("\n[9/9] Running Analysis 9: Category Performance Matrix...")
            result9 = self.run_analysis_9(**config['analysis_9'])

            print("\n" + "="*80)
            print(" " * 20 + "✓ ALL ANALYSES COMPLETED")
            print("="*80)

        except Exception as e:
            print(f"\n✗ Error during analysis execution: {str(e)}")
            traceback.print_exc()
            raise


def print_header():
    """Print application header"""
    print("\n" + "="*80)
    print(" " * 15 + "YELP BIG DATA ANALYSIS WITH KIBANA")
    print(" " * 20 + "Batch Mode + Elasticsearch Integration")
    print(" " * 25 + f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)


def print_footer(success=True):
    """Print application footer"""
    print("\n" + "="*80)
    if success:
        print(" " * 25 + "✓ PIPELINE COMPLETED SUCCESSFULLY")
        print("\n" + " " * 20 + "📊 View results in Kibana:")
        print(" " * 25 + "http://localhost:5601")
    else:
        print(" " * 25 + "✗ PIPELINE FAILED")
    print("="*80 + "\n")


def main():
    """Main execution function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Yelp Big Data Analysis with Elasticsearch & Kibana'
    )
    parser.add_argument(
        '--data-path',
        type=str,
        default='./data/',
        help='Path to data directory (default: ./data/)'
    )
    parser.add_argument(
        '--output-path',
        type=str,
        default='./output_elasticsearch/',
        help='Path to output directory (default: ./output_elasticsearch/)'
    )
    parser.add_argument(
        '--es-host',
        type=str,
        default='localhost',
        help='Elasticsearch hostname (default: localhost)'
    )
    parser.add_argument(
        '--es-port',
        type=int,
        default=9200,
        help='Elasticsearch port (default: 9200)'
    )
    parser.add_argument(
        '--init-indices',
        action='store_true',
        help='Initialize Elasticsearch indices before running'
    )

    args = parser.parse_args()

    # Print header
    print_header()

    try:
        # Initialize indices if requested
        if args.init_indices:
            print("\nInitializing Elasticsearch indices...")
            initialize_all_indices(args.es_host, args.es_port)

        # Create pipeline
        print(f"\nInitializing pipeline...")
        print(f"  Data Path: {args.data_path}")
        print(f"  Output Path: {args.output_path}")
        print(f"  Elasticsearch: {args.es_host}:{args.es_port}")

        pipeline = EnhancedYelpPipelineWithES(
            data_path=args.data_path,
            output_path=args.output_path,
            es_host=args.es_host,
            es_port=args.es_port
        )

        # Load data
        pipeline.load_data()

        # Run all analyses
        pipeline.run_all_analyses_with_es()

        # Save results to CSV (optional)
        print("\nSaving results to CSV...")
        pipeline.save_results()

        # Print summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"✓ Analyses completed: 9")
        print(f"✓ CSV outputs: {args.output_path}")
        if pipeline.es_enabled:
            print(f"✓ Elasticsearch indices: 9")
            print(f"✓ Kibana dashboard: http://localhost:5601")
        print("="*80)

        # Stop Spark
        pipeline.stop()

        # Print footer
        print_footer(success=True)

        return 0

    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        traceback.print_exc()
        print_footer(success=False)
        return 1


if __name__ == "__main__":
    sys.exit(main())
