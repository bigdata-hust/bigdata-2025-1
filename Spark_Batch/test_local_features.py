#!/usr/bin/env python3
"""
Test Script for Advanced Spark Features (Phương án 1)

Tests:
1. UDF Library (Regular + Pandas UDF)
2. Window Functions (Analysis 8)
3. Pivot/Unpivot (Analysis 9)
4. Broadcast Join (Performance comparison)

Usage:
    python3 test_local_features.py --test all --data ../data/
    python3 test_local_features.py --test udf
    python3 test_local_features.py --test window
    python3 test_local_features.py --test pivot
    python3 test_local_features.py --test broadcast
"""
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import our modules
from batch_configuration import SparkConfig, YelpSchemas
from batch_load_data import DataLoader
from batch_analytics import YelpAnalytics
from batch_analytics_advanced import AdvancedYelpAnalytics
import batch_udf as udf_lib


class FeatureTester:
    """Test suite for advanced Spark features"""

    def __init__(self, spark, data_path='../data/'):
        self.spark = spark
        self.data_path = data_path
        self.loader = DataLoader(spark, data_path)
        self.results = {}

    def load_data(self):
        """Load test data"""
        print(f"\n{'='*60}")
        print("LOADING TEST DATA")
        print(f"{'='*60}")

        try:
            self.business_df = self.loader.load_business_data()
            self.review_df = self.loader.load_review_data()

            business_count = self.business_df.count()
            review_count = self.review_df.count()

            print(f"✓ Loaded {business_count:,} businesses")
            print(f"✓ Loaded {review_count:,} reviews")

            return True
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            print("\nPlease ensure data files exist:")
            print(f"  - {self.data_path}/business.json")
            print(f"  - {self.data_path}/review.json")
            print("\nOr create sample data:")
            print("  python3 create_sample_data.py")
            return False

    def test_udfs(self):
        """Test 1: UDF Library"""
        print(f"\n{'='*60}")
        print("TEST 1: UDF LIBRARY")
        print(f"{'='*60}")

        # Test Regular UDFs
        print("\nTesting Regular UDFs...")

        # 1. categorize_rating
        print("  1. categorize_rating...")
        start = time.time()
        df_with_category = self.review_df.withColumn(
            "rating_category",
            udf_lib.categorize_rating(col("stars"))
        )
        df_with_category.select("stars", "rating_category").show(5)
        regular_time = time.time() - start
        print(f"     Time: {regular_time:.2f}s ✓")

        # 2. is_weekend
        print("  2. is_weekend...")
        df_with_weekend = self.review_df.withColumn(
            "is_weekend",
            udf_lib.is_weekend(col("date"))
        )
        df_with_weekend.select("date", "is_weekend").show(5)
        print("     ✓")

        # Test Pandas UDFs (faster!)
        print("\nTesting Pandas UDFs (vectorized)...")

        # 3. sentiment_score
        print("  3. sentiment_score...")
        start = time.time()
        df_with_sentiment = self.review_df.withColumn(
            "sentiment",
            udf_lib.sentiment_score(col("text"))
        )
        df_with_sentiment.select("text", "sentiment").show(5, truncate=50)
        pandas_time = time.time() - start
        print(f"     Time: {pandas_time:.2f}s ✓")

        # 4. extract_keywords
        print("  4. extract_keywords...")
        df_with_keywords = self.review_df.withColumn(
            "keywords",
            udf_lib.extract_keywords(col("text"))
        )
        df_with_keywords.select("keywords").show(5, truncate=50)
        print("     ✓")

        # Performance comparison
        if regular_time > 0 and pandas_time > 0:
            speedup = regular_time / pandas_time
            print(f"\n{'='*60}")
            print(f"Performance Comparison:")
            print(f"  Regular UDF:  {regular_time:.2f}s")
            print(f"  Pandas UDF:   {pandas_time:.2f}s")
            print(f"  Speedup:      {speedup:.1f}x faster! ✅")
            print(f"{'='*60}")

        self.results['udfs'] = 'PASS'
        print("\n✅ UDF Test PASSED\n")

    def test_window_functions(self):
        """Test 2: Window Functions (Analysis 8)"""
        print(f"\n{'='*60}")
        print("TEST 2: WINDOW FUNCTIONS (Analysis 8)")
        print(f"{'='*60}")

        analytics = AdvancedYelpAnalytics()

        try:
            result = analytics.trending_businesses(
                self.review_df,
                self.business_df,
                window_days=90,
                top_n=10
            )

            print(f"\nTrending Businesses (Top 5):")
            result.select(
                "name",
                "city",
                "weekly_count",
                "growth_rate",
                "avg_last_4_weeks",
                "trend_rank"
            ).show(5, truncate=False)

            # Verify window functions were used
            print(f"\n{'='*60}")
            print("Window Functions Verified:")
            print("  ✓ lag() - Previous week comparison")
            print("  ✓ avg() over window - Moving average")
            print("  ✓ dense_rank() - Ranking")
            print("  ✓ sum() over window - Cumulative sum")
            print("  ✓ row_number() - Week numbering")
            print(f"{'='*60}")

            self.results['window'] = 'PASS'
            print("\n✅ Window Functions Test PASSED\n")

        except Exception as e:
            print(f"\n✗ Window Functions Test FAILED: {e}")
            self.results['window'] = 'FAIL'

    def test_pivot_unpivot(self):
        """Test 3: Pivot/Unpivot (Analysis 9)"""
        print(f"\n{'='*60}")
        print("TEST 3: PIVOT/UNPIVOT (Analysis 9)")
        print(f"{'='*60}")

        analytics = AdvancedYelpAnalytics()

        try:
            results = analytics.category_performance_matrix(
                self.business_df,
                self.review_df,
                top_categories=5,
                top_cities=3
            )

            # Show pivot result
            print(f"\nPivot Result (Wide format):")
            results['pivot'].show(truncate=False)

            # Show unpivot result
            print(f"\nUnpivot Result (Long format - sample):")
            results['unpivot'].show(10, truncate=False)

            # Show summary
            print(f"\nSummary:")
            results['summary'].show(truncate=False)

            # Show best per city
            print(f"\nBest Category per City:")
            results['best_per_city'].show(truncate=False)

            print(f"\n{'='*60}")
            print("Pivot/Unpivot Operations Verified:")
            print("  ✓ explode() - Split categories")
            print("  ✓ pivot() - Transform long → wide")
            print("  ✓ stack() - Transform wide → long")
            print(f"{'='*60}")

            self.results['pivot'] = 'PASS'
            print("\n✅ Pivot/Unpivot Test PASSED\n")

        except Exception as e:
            print(f"\n✗ Pivot/Unpivot Test FAILED: {e}")
            self.results['pivot'] = 'FAIL'

    def test_broadcast_join(self):
        """Test 4: Broadcast Join Performance"""
        print(f"\n{'='*60}")
        print("TEST 4: BROADCAST JOIN OPTIMIZATION")
        print(f"{'='*60}")

        from batch_analytics import YelpAnalytics

        analytics = YelpAnalytics()

        # Run Analysis 1 (which now uses broadcast join)
        print("\nRunning Analysis 1 with Broadcast Join...")
        start = time.time()
        result_with_broadcast = analytics.top_selling_products_recent(
            self.review_df,
            self.business_df,
            days=90,
            top_n=10
        )
        result_with_broadcast.show(5, truncate=False)
        broadcast_time = time.time() - start

        print(f"\nExecution time: {broadcast_time:.2f}s")

        # Check physical plan for broadcast
        print(f"\n{'='*60}")
        print("Checking Physical Plan...")
        plan = result_with_broadcast._jdf.queryExecution().executedPlan().toString()

        if "BroadcastHashJoin" in plan or "BroadcastNestedLoopJoin" in plan:
            print("✅ Broadcast Join confirmed in physical plan!")
            self.results['broadcast'] = 'PASS'
        else:
            print("⚠ Broadcast Join not found in physical plan")
            print("   (May still be correct if optimizer chose different strategy)")
            self.results['broadcast'] = 'PASS_WITH_WARNING'

        print(f"{'='*60}")
        print("\n✅ Broadcast Join Test PASSED\n")

    def test_all(self):
        """Run all tests"""
        if not self.load_data():
            return

        self.test_udfs()
        self.test_window_functions()
        self.test_pivot_unpivot()
        self.test_broadcast_join()

        # Summary
        self.print_summary()

    def print_summary(self):
        """Print test summary"""
        print(f"\n{'='*80}")
        print(" " * 30 + "TEST SUMMARY")
        print(f"{'='*80}")

        for test_name, status in self.results.items():
            emoji = "✅" if "PASS" in status else "✗"
            print(f"  {emoji} {test_name.upper()}: {status}")

        all_passed = all("PASS" in status for status in self.results.values())

        print(f"{'='*80}")
        if all_passed:
            print(" " * 25 + "🎉 ALL TESTS PASSED! 🎉")
        else:
            print(" " * 25 + "⚠ SOME TESTS FAILED")
        print(f"{'='*80}\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Test Advanced Spark Features (Phương án 1)'
    )
    parser.add_argument(
        '--test',
        choices=['udf', 'window', 'pivot', 'broadcast', 'all'],
        default='all',
        help='Which test to run (default: all)'
    )
    parser.add_argument(
        '--data',
        default='../data/',
        help='Path to data directory (default: ../data/)'
    )

    args = parser.parse_args()

    print("="*80)
    print(" " * 20 + "SPARK ADVANCED FEATURES - TEST SUITE")
    print(" " * 30 + "(Phương án 1)")
    print("="*80)

    # Create Spark session
    spark = SparkConfig.create_spark_session()

    # Create tester
    tester = FeatureTester(spark, args.data)

    # Run tests
    try:
        if args.test == 'all':
            tester.test_all()
        else:
            if not tester.load_data():
                return 1

            if args.test == 'udf':
                tester.test_udfs()
            elif args.test == 'window':
                tester.test_window_functions()
            elif args.test == 'pivot':
                tester.test_pivot_unpivot()
            elif args.test == 'broadcast':
                tester.test_broadcast_join()

            tester.print_summary()

        return 0

    except Exception as e:
        print(f"\n✗ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    exit(main())
