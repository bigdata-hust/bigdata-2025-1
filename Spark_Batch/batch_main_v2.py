#!/usr/bin/env python3
"""
Yelp Big Data Analysis System - Version 2 (Enhanced with Advanced Features)
Main Entry Point for Local Execution with 9 Analyses

NEW in v2:
- Analysis 8: Trending Businesses (Window Functions)
- Analysis 9: Category Performance Matrix (Pivot/Unpivot)
- Broadcast Join optimization for 7 original analyses
- UDF library support

Usage:
    python3 batch_main_v2.py [--data-path DATA_PATH] [--output-path OUTPUT_PATH]

Examples:
    python3 batch_main_v2.py
    python3 batch_main_v2.py --data-path ./data/
    python3 batch_main_v2.py --data-path /path/to/data/ --show-rows 20
"""
import sys
import argparse
import traceback
from datetime import datetime

from batch_pipeline import YelpAnalysisPipeline
from batch_analytics_advanced import AdvancedYelpAnalytics
from batch_load_data import DataLoader
from batch_configuration import SparkConfig


def print_header():
    """Print application header"""
    print("\n" + "="*80)
    print(" " * 15 + "YELP BIG DATA ANALYSIS SYSTEM - VERSION 2")
    print(" " * 20 + "BATCH MODE - Enhanced with Advanced Features")
    print(" " * 28 + f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    print("\nNEW FEATURES:")
    print("  ✨ Window Functions (Analysis 8: Trending Businesses)")
    print("  ✨ Pivot/Unpivot Operations (Analysis 9: Performance Matrix)")
    print("  ✨ Broadcast Join Optimization (Analyses 1-7)")
    print("  ✨ UDF Library Support (7 custom functions)")
    print("="*80)


def print_footer(success=True):
    """Print application footer"""
    print("\n" + "="*80)
    if success:
        print(" " * 25 + "✓ PIPELINE COMPLETED SUCCESSFULLY")
        print(" " * 30 + "9 Analyses Executed")
    else:
        print(" " * 25 + "✗ PIPELINE FAILED")
    print("="*80 + "\n")


class EnhancedYelpPipeline(YelpAnalysisPipeline):
    """Enhanced pipeline with 9 analyses (7 original + 2 new)"""

    def __init__(self, data_path=None, output_path=None):
        super().__init__(data_path, output_path)
        self.advanced_analytics = AdvancedYelpAnalytics()

    def run_analysis_8(self, window_days=90, top_n=10):
        """Run Analysis 8: Trending Businesses (Window Functions)"""
        try:
            result = self.advanced_analytics.trending_businesses(
                self.review_df,
                self.business_df,
                window_days=window_days,
                top_n=top_n
            )
            self.results['trending_businesses'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 8: {str(e)}")
            raise

    def run_analysis_9(self, top_categories=10, top_cities=5):
        """Run Analysis 9: Category Performance Matrix (Pivot/Unpivot)"""
        try:
            result_dict = self.advanced_analytics.category_performance_matrix(
                self.business_df,
                self.review_df,
                top_categories=top_categories,
                top_cities=top_cities
            )
            # Store all results
            self.results['category_matrix_pivot'] = result_dict['pivot']
            self.results['category_matrix_unpivot'] = result_dict['unpivot']
            self.results['category_summary'] = result_dict['summary']
            self.results['best_per_city'] = result_dict['best_per_city']
            self.results['best_per_category'] = result_dict['best_per_category']
            return result_dict
        except Exception as e:
            print(f"✗ Error in Analysis 9: {str(e)}")
            raise

    def run_all_analyses_v2(self, config=None):
        """Run all 9 analyses (7 original + 2 new)"""
        if config is None:
            config = {
                'analysis_1': {'days': 90, 'top_n': 10},
                'analysis_2': {'top_n': 10},
                'analysis_3': {'min_reviews': 10, 'top_n': 10},
                'analysis_4': {'positive_threshold': 4, 'top_n': 10},
                'analysis_6': {'top_n': 20},
                'analysis_8': {'window_days': 90, 'top_n': 10},
                'analysis_9': {'top_categories': 10, 'top_cities': 5}
            }

        print("\n" + "="*60)
        print("ANALYSIS PHASE - RUNNING ALL 9 ANALYSES")
        print("="*60)
        print(f"\nOriginal analyses (1-7): Using Broadcast Join optimization")
        print(f"New analyses (8-9): Using Window Functions & Pivot/Unpivot")
        print("="*60)

        import time
        total_start = time.time()

        # Run original 7 analyses (now with broadcast join)
        print(f"\n{'='*60}")
        print("PART 1: Original Analyses (1-7)")
        print(f"{'='*60}")

        self.run_analysis_1(**config.get('analysis_1', {}))
        self.run_analysis_2(**config.get('analysis_2', {}))
        self.run_analysis_3(**config.get('analysis_3', {}))
        self.run_analysis_4(**config.get('analysis_4', {}))
        self.run_analysis_5()
        self.run_analysis_6(**config.get('analysis_6', {}))
        self.run_analysis_7()

        # Run new analyses
        print(f"\n{'='*60}")
        print("PART 2: Advanced Analyses (8-9)")
        print(f"{'='*60}")

        self.run_analysis_8(**config.get('analysis_8', {}))
        self.run_analysis_9(**config.get('analysis_9', {}))

        total_elapsed = time.time() - total_start
        print("\n" + "="*60)
        print(f"ALL 9 ANALYSES COMPLETED in {total_elapsed:.2f}s")
        print("="*60)

    def display_results_v2(self, show_rows=10):
        """Display all results including new analyses"""
        print("\n" + "="*80)
        print(" " * 25 + "RESULTS PREVIEW - 9 ANALYSES")
        print("="*80)

        # Original 7 analyses
        print(f"\n{'='*80}")
        print("PART 1: Original Analyses (1-7)")
        print(f"{'='*80}")

        for name in ['top_selling', 'diverse_stores', 'best_rated', 'most_positive',
                     'peak_hours', 'top_categories', 'store_stats']:
            if name in self.results:
                print(f"\n{' ' + name.upper().replace('_', ' ') + ' ':=^80}")
                self.results[name].show(show_rows, truncate=False)

        # New analyses
        print(f"\n{'='*80}")
        print("PART 2: Advanced Analyses (8-9)")
        print(f"{'='*80}")

        # Analysis 8: Trending
        if 'trending_businesses' in self.results:
            print(f"\n{' ANALYSIS 8: TRENDING BUSINESSES ':=^80}")
            self.results['trending_businesses'].show(show_rows, truncate=False)

        # Analysis 9: Category Matrix
        if 'category_matrix_pivot' in self.results:
            print(f"\n{' ANALYSIS 9A: CATEGORY MATRIX (PIVOT) ':=^80}")
            self.results['category_matrix_pivot'].show(truncate=False)

            print(f"\n{' ANALYSIS 9B: CATEGORY MATRIX (UNPIVOT - SAMPLE) ':=^80}")
            self.results['category_matrix_unpivot'].show(show_rows, truncate=False)

            print(f"\n{' ANALYSIS 9C: BEST CATEGORY PER CITY ':=^80}")
            self.results['best_per_city'].show(truncate=False)

    def generate_summary_report_v2(self):
        """Generate enhanced summary statistics"""
        print("\n" + "="*80)
        print(" " * 30 + "SUMMARY REPORT - V2")
        print("="*80)

        print(f"\n{'='*60}")
        print("Part 1: Original Analyses (1-7)")
        print(f"{'='*60}")

        for name in ['top_selling', 'diverse_stores', 'best_rated', 'most_positive',
                     'peak_hours', 'top_categories', 'store_stats']:
            if name in self.results:
                count = self.results[name].count()
                print(f"  {name.upper().replace('_', ' ')}: {count:,} records")

        print(f"\n{'='*60}")
        print("Part 2: Advanced Analyses (8-9)")
        print(f"{'='*60}")

        if 'trending_businesses' in self.results:
            count = self.results['trending_businesses'].count()
            print(f"  TRENDING BUSINESSES: {count:,} businesses")

        if 'category_matrix_unpivot' in self.results:
            count = self.results['category_matrix_unpivot'].count()
            print(f"  CATEGORY MATRIX: {count:,} category-city pairs")

        if 'category_summary' in self.results:
            summary = self.results['category_summary'].collect()[0]
            print(f"  CATEGORIES ANALYZED: {summary['total_categories']}")
            print(f"  CITIES ANALYZED: {summary['total_cities']}")
            print(f"  TOTAL REVIEWS: {summary['total_reviews']:,}")

        print("="*60)

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Yelp Big Data Analysis - Enhanced Version 2'
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
        default='./output_v2/',
        help='Path to output directory (default: ./output_v2/)'
    )
    parser.add_argument(
        '--show-rows',
        type=int,
        default=10,
        help='Number of rows to display in results (default: 10)'
    )
    parser.add_argument(
        '--save-format',
        type=str,
        default='parquet',
        choices=['parquet', 'csv', 'json'],
        help='Output format for results (default: parquet)'
    )
    parser.add_argument(
        '--skip-save',
        action='store_true',
        help='Skip saving results to disk'
    )

    args = parser.parse_args()

    # Print header
    print_header()

    # Initialize enhanced pipeline
    pipeline = EnhancedYelpPipeline(
        data_path=args.data_path,
        output_path=args.output_path
    )

    try:
        # Step 1: Load data
        pipeline.load_data()

        # Step 2: Run all 9 analyses
        pipeline.run_all_analyses_v2(config={
            'analysis_1': {'days': 90, 'top_n': 10},
            'analysis_2': {'top_n': 10},
            'analysis_3': {'min_reviews': 10, 'top_n': 10},
            'analysis_4': {'positive_threshold': 4, 'top_n': 10},
            'analysis_6': {'top_n': 20},
            'analysis_8': {'window_days': 90, 'top_n': 10},
            'analysis_9': {'top_categories': 10, 'top_cities': 5}
        })

        # Step 3: Display results
        pipeline.display_results_v2(show_rows=args.show_rows)

        # Step 4: Save results (optional)
        if not args.skip_save:
            pipeline.save_results(format=args.save_format)

        # Step 5: Generate summary
        pipeline.generate_summary_report_v2()

        # Step 6: Cleanup
        pipeline.cleanup()

        # Print footer
        print_footer(success=True)

        # Stop Spark
        pipeline.stop()

        return 0

    except FileNotFoundError as e:
        print(f"\n✗ Data file not found: {str(e)}")
        print("\nPlease ensure your data files are in the correct location:")
        print(f"  - business.json in {args.data_path}")
        print(f"  - review.json in {args.data_path}")
        print("\nOr create sample data:")
        print("  python3 create_sample_data.py")
        print_footer(success=False)
        return 1

    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        traceback.print_exc()
        print_footer(success=False)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
