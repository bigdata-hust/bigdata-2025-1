#!/usr/bin/env python3
"""
Yelp Big Data Analysis System - BATCH MODE
Main Entry Point for Local Execution

Usage:
    python batch_main.py [--data-path DATA_PATH] [--output-path OUTPUT_PATH]

Examples:
    python batch_main.py
    python batch_main.py --data-path ./data/
    python batch_main.py --data-path /path/to/data/ --output-path ./results/
"""
import sys
import argparse
import traceback
from datetime import datetime

from batch_pipeline import YelpAnalysisPipeline


def print_header():
    """Print application header"""
    print("\n" + "="*80)
    print(" " * 20 + "YELP BIG DATA ANALYSIS SYSTEM")
    print(" " * 25 + "BATCH MODE - Local Execution")
    print(" " * 28 + f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)


def print_footer(success=True):
    """Print application footer"""
    print("\n" + "="*80)
    if success:
        print(" " * 25 + "✓ PIPELINE COMPLETED SUCCESSFULLY")
    else:
        print(" " * 25 + "✗ PIPELINE FAILED")
    print("="*80 + "\n")


def main():
    """Main execution function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Yelp Big Data Analysis - Batch Mode'
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
        default='./output/',
        help='Path to output directory (default: ./output/)'
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

    # Initialize pipeline
    pipeline = YelpAnalysisPipeline(
        data_path=args.data_path,
        output_path=args.output_path
    )

    try:
        # Step 1: Load data
        pipeline.load_data()

        # Step 2: Run all analyses (1–7)
        pipeline.run_all_analyses(config={
            'analysis_1': {'days': 90, 'top_n': 10},
            'analysis_2': {'top_n': 10},
            'analysis_3': {'min_reviews': 10, 'top_n': 10},
            'analysis_4': {'positive_threshold': 4, 'top_n': 10},
            'analysis_6': {'top_n': 20}
        })

        # Step 3: Display results
        pipeline.display_results(show_rows=args.show_rows)

        # Step 4: Save results (optional)
        if not args.skip_save:
            pipeline.save_results(format=args.save_format)

        # Step 5: Generate summary
        pipeline.generate_summary_report()

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
        print_footer(success=False)
        return 1

    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        traceback.print_exc()
        print_footer(success=False)
        return 1


def run_single_analysis(analysis_number, data_path='./data/', **kwargs):
    """
    Run a single analysis independently

    Args:
        analysis_number: 1–7
        data_path: path to data directory
        **kwargs: parameters for the specific analysis

    Example:
        run_single_analysis(1, data_path='./data/', days=30, top_n=5)
    """
    pipeline = YelpAnalysisPipeline(data_path=data_path)

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
        elif analysis_number == 5:
            result = pipeline.run_analysis_5()
        elif analysis_number == 6:
            result = pipeline.run_analysis_6(**kwargs)
        elif analysis_number == 7:
            result = pipeline.run_analysis_7()
        else:
            raise ValueError("Analysis number must be between 1 and 7")

        print(f"\n✓ Analysis {analysis_number} completed successfully!")
        result.show(20, truncate=False)
        return result

    except Exception as e:
        print(f"✗ Error running analysis {analysis_number}: {str(e)}")
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run full pipeline
    exit_code = main()
    sys.exit(exit_code)
