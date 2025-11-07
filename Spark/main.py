# ============================================================================
# MAIN EXECUTION
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
from configuration import SparkConfig
from pipeline_orchestration import YelpAnalysisPipeline

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
        data_path="../bigdata-2025-1/data/",
        output_path="output/"
    )
    
    try:
        # Step 1: Load data
        pipeline.load_data()
        
        # Step 2: Run all analyses (1–7)
        pipeline.run_all_analyses(config={
            'analysis_1': {'days': 15, 'top_n': 10},
            'analysis_2': {'top_n': 10},
            'analysis_3': {'min_reviews': 10, 'top_n': 10},
            'analysis_4': {'positive_threshold': 4, 'top_n': 10},
            'analysis_6': {'top_n': 20}
        })
        
        # Step 3: Display results
        pipeline.display_results()
        
        # Step 4: Save results
        # Step 5 : Save HDFS
        # Step 6 : Save ElasticSearch
        # Step 7 : Save MongoDB
        pipeline.save_all()

        # Step 8: Generate summary
        pipeline.generate_summary_report()
        
        # Step 9: Cleanup
        pipeline.cleanup()
        
        print("\n" + "="*80)
        print(" " * 25 + "PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80 + "\n")
    
    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
   


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def run_single_analysis(analysis_number, **kwargs):
    """
    Run a single analysis independently
    
    Args:
        analysis_number: 1–7
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
        elif analysis_number == 5:
            result = pipeline.run_analysis_5()
        elif analysis_number == 6:
            result = pipeline.run_analysis_6(**kwargs)
        elif analysis_number == 7:
            result = pipeline.run_analysis_7()
        else:
            raise ValueError("Analysis number must be between 1 and 7")
        
        print(f"\n✓ Analysis {analysis_number} completed successfully!")
        result.show(truncate=False)
        return result
    
    except Exception as e:
        print(f"✗ Error running analysis {analysis_number}: {str(e)}")
        import traceback
        traceback.print_exc()
    



def run_custom_analysis(business_df, review_df, analysis_func, **kwargs):
    """
    Run custom analysis function (standalone)
    
    Args:
        business_df: business DataFrame
        review_df: review DataFrame
        analysis_func: custom analysis function (e.g. YelpAnalytics.get_top_categories)
        **kwargs: parameters for the analysis function
    """
    print("\n" + "="*60)
    print(f"RUNNING CUSTOM ANALYSIS: {analysis_func.__name__}")
    print("="*60)
    try:
        result = analysis_func(business_df, review_df, **kwargs)
        result.show(truncate=False)
        return result
    except Exception as e:
        print(f"✗ Error in custom analysis: {str(e)}")
        import traceback
        traceback.print_exc()

    
if __name__ == "__main__": # Run full pipeline 
    main() 
    
    