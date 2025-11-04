#!/usr/bin/env python3
"""
Main Pipeline Runner cho Spark-Elasticsearch-Kibana
Phiên bản tối ưu cho CSV data
"""

import sys
import os
from datetime import datetime
import time

# Import các module cần thiết
from spark_elasticsearch_integration import (
    ElasticsearchConfig,
    SparkESSession,
    SparkToElasticsearch,
    ElasticsearchMappings,
    YelpElasticsearchPipeline
)

from csv_data_loader import YelpAnalyticsPipeline
from yelp_analytics import YelpAnalytics


def print_header(title):
    """In header đẹp"""
    print("\n" + "="*80)
    print(" " * ((80 - len(title)) // 2) + title)
    print("="*80 + "\n")


def check_elasticsearch_connection(es_config):
    """Kiểm tra kết nối Elasticsearch"""
    try:
        es_client = es_config.get_es_client()
        info = es_client.info()
        print(f"✓ Đã kết nối Elasticsearch version {info['version']['number']}")
        return True
    except Exception as e:
        print(f"✗ Không thể kết nối Elasticsearch: {str(e)}")
        print("\n💡 Hãy chạy: docker-compose up -d")
        return False


def run_analysis(spark_pipeline, config):
    """
    Chạy các phân tích Yelp
    
    Args:
        spark_pipeline: YelpAnalyticsPipeline instance
        config: Dict cấu hình cho các analyses
    
    Returns:
        Dict chứa results
    """
    business_df, review_df, user_df = spark_pipeline.get_dataframes()
    analytics = YelpAnalytics()
    results = {}
    
    print_header("CHẠY CÁC PHÂN TÍCH YELP")
    
    # Analysis 1: Top Selling Products
    try:
        print("1️⃣  Analysis 1: Top Selling Products...")
        results['top_selling'] = analytics.top_selling_products_recent(
            review_df, business_df,
            days=config.get('analysis_1', {}).get('days', 90),
            top_n=config.get('analysis_1', {}).get('top_n', 10)
        )
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    # Analysis 2: Diverse Stores
    try:
        print("2️⃣  Analysis 2: Most Diverse Stores...")
        results['diverse_stores'] = analytics.top_stores_by_product_count(
            business_df,
            top_n=config.get('analysis_2', {}).get('top_n', 10)
        )
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    # Analysis 3: Best Rated Products
    try:
        print("3️⃣  Analysis 3: Best Rated Products...")
        results['best_rated'] = analytics.top_rated_products(
            business_df, review_df,
            min_reviews=config.get('analysis_3', {}).get('min_reviews', 50),
            top_n=config.get('analysis_3', {}).get('top_n', 10)
        )
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    # Analysis 4: Most Positive Reviews
    try:
        print("4️⃣  Analysis 4: Stores with Most Positive Reviews...")
        results['most_positive'] = analytics.top_stores_by_positive_reviews(
            business_df, review_df,
            positive_threshold=config.get('analysis_4', {}).get('positive_threshold', 4),
            top_n=config.get('analysis_4', {}).get('top_n', 10)
        )
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    # Analysis 5: Peak Hours
    try:
        print("5️⃣  Analysis 5: Peak Review Hours...")
        results['peak_hours'] = analytics.get_peak_hours(review_df)
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    # Analysis 6: Top Categories
    try:
        print("6️⃣  Analysis 6: Top Categories...")
        results['top_categories'] = analytics.get_top_categories(
            business_df, review_df,
            top_n=config.get('analysis_6', {}).get('top_n', 20)
        )
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    # Analysis 7: Store Statistics
    try:
        print("7️⃣  Analysis 7: Store Statistics...")
        results['store_stats'] = analytics.get_store_stats(business_df, review_df)
        print("   ✓ Completed")
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
    
    print(f"\n✓ Hoàn thành {len(results)}/7 analyses")
    return results


def main():
    """Main execution function"""
    
    print_header("YELP SPARK-ELASTICSEARCH-KIBANA PIPELINE")
    print(f"Bắt đầu lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_time = time.time()
    
    # ========================================================================
    # BƯỚC 1: KHỞI TẠO SPARK SESSION
    # ========================================================================
    print_header("BƯỚC 1: KHỞI TẠO SPARK SESSION")
    
    try:
        spark = SparkESSession.create_session()
        print("✓ Spark Session đã sẵn sàng")
    except Exception as e:
        print(f"✗ Lỗi khởi tạo Spark: {str(e)}")
        return False
    
    # ========================================================================
    # BƯỚC 2: CẤU HÌNH ELASTICSEARCH
    # ========================================================================
    print_header("BƯỚC 2: CẤU HÌNH ELASTICSEARCH")
    
    # Đọc config từ environment variables hoặc sử dụng default
    es_host = os.getenv('ES_HOST', 'localhost')
    es_port = os.getenv('ES_PORT', '9200')
    es_username = os.getenv('ES_USERNAME', None)
    es_password = os.getenv('ES_PASSWORD', None)
    
    es_config = ElasticsearchConfig(
        hosts=[f'{es_host}:{es_port}'],
        username=es_username,
        password=es_password,
        use_ssl=False,
        verify_certs=False
    )
    
    if not check_elasticsearch_connection(es_config):
        print("\n⚠️  Elasticsearch chưa sẵn sàng. Hãy khởi động trước:")
        print("   docker-compose up -d")
        print("\nBạn có muốn tiếp tục không? (Sẽ chỉ chạy phân tích, không export ES)")
        response = input("Tiếp tục? (y/n): ").strip().lower()
        if response != 'y':
            return False
        skip_es_export = True
    else:
        skip_es_export = False
    
    # ========================================================================
    # BƯỚC 3: LOAD DỮ LIỆU TỪ CSV
    # ========================================================================
    print_header("BƯỚC 3: LOAD DỮ LIỆU TỪ CSV")
    
    data_path = os.getenv('DATA_PATH', '../processed_data/')
    print(f"Data path: {data_path}")
    
    spark_pipeline = YelpAnalyticsPipeline(spark, data_path)
    
    if not spark_pipeline.load_all_data(validate=True):
        print("✗ Load data thất bại!")
        return False
    
    print("✓ Đã load tất cả dữ liệu thành công")
    
    # ========================================================================
    # BƯỚC 4: CHẠY CÁC PHÂN TÍCH
    # ========================================================================
    
    # Cấu hình cho các analyses
    analysis_config = {
        'analysis_1': {'days': 90, 'top_n': 10},
        'analysis_2': {'top_n': 10},
        'analysis_3': {'min_reviews': 50, 'top_n': 10},
        'analysis_4': {'positive_threshold': 4, 'top_n': 10},
        'analysis_6': {'top_n': 20}
    }
    
    results = run_analysis(spark_pipeline, analysis_config)
    
    if not results:
        print("✗ Không có kết quả phân tích nào!")
        return False
    
    # ========================================================================
    # BƯỚC 5: EXPORT SANG ELASTICSEARCH
    # ========================================================================
    
    if not skip_es_export:
        print_header("BƯỚC 5: EXPORT SANG ELASTICSEARCH")
        
        try:
            # Tạo YelpElasticsearchPipeline wrapper
            class ResultsWrapper:
                """Wrapper để có interface tương thích"""
                def __init__(self, results):
                    self.results = results
            
            wrapper = ResultsWrapper(results)
            
            es_pipeline = YelpElasticsearchPipeline(spark, es_config, wrapper)
            es_pipeline.export_all_analyses()
            
            print("✓ Export sang Elasticsearch hoàn tất!")
            
        except Exception as e:
            print(f"✗ Lỗi khi export: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # ========================================================================
    # BƯỚC 6: HIỂN THỊ KẾT QUẢ
    # ========================================================================
    print_header("BƯỚC 6: KẾT QUẢ PHÂN TÍCH")
    
    for key, df in results.items():
        print(f"\n{key}:")
        try:
            df.show(5, truncate=False)
        except:
            print(f"  Đã lưu kết quả (count: {df.count()})")
    
    # ========================================================================
    # HOÀN TẤT
    # ========================================================================
    
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    
    print_header("HOÀN TẤT!")
    print(f"Tổng thời gian: {minutes} phút {seconds} giây")
    print(f"Kết thúc lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not skip_es_export:
        print("\n📊 Bước tiếp theo:")
        print("  1. Mở Kibana: http://localhost:5601")
        print("  2. Vào Stack Management > Index Patterns")
        print("  3. Tạo index patterns cho các index sau:")
        print("     - yelp-top-selling*")
        print("     - yelp-diverse-stores*")
        print("     - yelp-best-rated*")
        print("     - yelp-positive-reviews*")
        print("     - yelp-peak-hours*")
        print("     - yelp-top-categories*")
        print("     - yelp-store-stats*")
        print("  4. Tạo Visualizations và Dashboards")
    
    # Cleanup
    print("\n🛑 Dừng Spark Session...")
    spark.stop()
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline bị dừng bởi người dùng")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Lỗi không mong đợi: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
