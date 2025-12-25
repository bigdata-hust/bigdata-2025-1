# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""
import os
from pyspark.sql.functions import *
from functools import partial
import time

from analytics_yelp import YelpAnalytics
from load_data import DataLoader
from configuration import SparkConfig
import requests
import json
import traceback
from configuration import SparkConfig

def save_es(df, batch_id, index):
    print('\n' + '='*60)
    print(f'=== Start save {index} to ElasticSearch (DRIVER CHUNKING MODE) ===')
    print('='*60)
    print('=== batch id : ', str(batch_id), " ===")
    
    elastic_uri = os.getenv("ELASTIC_URI", "http://elasticsearch:9200")
    
    # 1. LẤY ITERATOR: Dữ liệu sẽ được tải về từng dòng một, không tải hết 1 cục
    try:
        row_iterator = df.toLocalIterator()
    except Exception as e:
        print(f"❌ Error getting iterator: {e}")
        return

    # Cấu hình kích thước mỗi gói tin gửi đi (Ví dụ: 2000 dòng/gói)
    CHUNK_SIZE = 2000 
    current_chunk = []
    total_count = 0

    # 2. DUYỆT VÀ GỬI CUỐN CHIẾU (STREAMING)
    # Không dùng len() hay gom hết vào 1 biến string to
    try:
        for row in row_iterator:
            current_chunk.append(row)
            
            # Nếu gom đủ 2000 dòng thì gửi đi ngay và xóa bộ nhớ
            if len(current_chunk) >= CHUNK_SIZE:
                push_chunk_to_es(current_chunk, elastic_uri, index)
                total_count += len(current_chunk)
                current_chunk = [] # Reset list để giải phóng RAM
        
        # Gửi nốt số lẻ còn lại (nếu có)
        if current_chunk:
            push_chunk_to_es(current_chunk, elastic_uri, index)
            total_count += len(current_chunk)
            
        print(f"✅ Finished Batch {batch_id}. Total pushed: {total_count} records.")
        
    except Exception as e:
        print(f"❌ Error processing iterator: {e}")

# Hàm phụ trợ để gửi từng gói nhỏ (Giúp code gọn hơn)
def push_chunk_to_es(rows, uri, index):
    if not rows: return

    bulk_data = ""
    for r in rows:
        doc = r.asDict(recursive=True)
        bulk_data += json.dumps({"index": {}}) + "\n"
        # Xử lý format ngày tháng nếu cần
        bulk_data += json.dumps(
            doc,
            default=lambda x: x.isoformat() if hasattr(x, "isoformat") else x
        ) + "\n"
    
    # Gửi request
    try:
        url = f"{uri}/{index}/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}
        res = requests.post(url, data=bulk_data, headers=headers)
        
        if res.status_code >= 300:
            print(f"⚠️ Chunk Error: {res.status_code} - {res.text[:100]}...")
        # else:
            # print(f"   -> Pushed chunk {len(rows)} items.") # Uncomment nếu muốn debug chi tiết
            
    except Exception as e:
        print(f"❌ Connection Error sending chunk: {e}")
        
        
class YelpAnalysisPipeline:
    """
    Main pipeline orchestrator
    Production-ready with error handling, monitoring, and checkpointing
    """
    
    def __init__(self, data_path=None, output_path=None):
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
        self.user_df = self.data_loader.load_user_data()  
        
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

    def run_analysis_8(self):
        """Run Analysis 8: Yelp city sentiment Summary"""
        try:
            result = self.analytics.yelp_city_sentiment_summary(self.business_df, self.review_df , self.user_df)
            self.results['city_sentiment'] = result
            return result
        except Exception as e:
            print(f"✗ Error in Analysis 8: {str(e)}")
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

        
        self.run_analysis_1(**config['analysis_1'])
        self.run_analysis_2(**config['analysis_2'])
        self.run_analysis_3(**config['analysis_3'])
        self.run_analysis_4(**config['analysis_4'])
        self.run_analysis_5()
        self.run_analysis_6(**config['analysis_6'])
        self.run_analysis_7()
        self.run_analysis_8()

        

        result3 = self.run_analysis_3(**config.get("analysis_3", {}))
        
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
           
    
    from pyspark.sql.functions import current_timestamp

    

    def save_results(self):
        """
        Save results to disk
        
        Args:
            format: output format ('parquet', 'csv', 'json')
            coalesce: whether to coalesce to single file
        """
        print("\n" + "="*60)
        print("SAVING RESULTS")
        print("="*60)
        hdfs_host = os.getenv("HDFS_URI", "hdfs://hdfs-namenode:9000")

        for name, df in self.results.items():
            output_path = f"{self.output_path}{name}"
            try :
                df.writeStream.format('parquet') \
                                .outputMode('append') \
                                .option('checkpointLocation', f"{hdfs_host}/check_output_dir1/{name}")
                print(f"✓ Saved {name} to {output_path}")
            
            except Exception as e:
                print(f"✗ Error saving {name}: {str(e)}")

    def save_hdfs(self):
        import uuid
        print('\n' + '='*60)
        print('SAVING TO HDFS')
        print('='*60)
        queries = []
        hdfs_host = os.getenv("HDFS_URI", "hdfs://hdfs-namenode:9000")
        for name, df in self.results.items():
            output = f"{hdfs_host}/yelp-sentiment/analytics/{name}"
            try:
                df_partitions = (df
                    .withColumn('created_date', current_timestamp())
                    .withColumn('updated_date', current_timestamp())
                    .withColumn('year', year('created_date'))
                    .withColumn('month', month('created_date'))
                    .withColumn('day', dayofmonth('created_date'))
                    .withColumn('hour', hour('created_date'))
                )

                query = (
                df_partitions.writeStream
                    .format('parquet')
                    .outputMode('append')
                    .partitionBy('year', 'month', 'day', 'hour')
                    .option('path', output)
                    .option('checkpointLocation', f"{hdfs_host}/check_point_dir/{name}/{uuid.uuid4()}")
                    .option("compression", "snappy")
                    .trigger(processingTime="3 minute")
                    .start()
                )

                queries.append(query)
                print(f" Stream '{name}' started -> {output}")
            except Exception as e:
                print(f" Error saving {name} to HDFS: {e}")
        return queries
            
    def save_elasticsearch(self) :
        print('\n' + '='*60)
        print('SAVING TO ELASTICSEARCH')
        print('='*60)
        
        queries = []
        for name,df in self.results.items() :
            try :
                query = (
                    df.writeStream.foreachBatch(partial(save_es , index = name)) \
                                    .outputMode('append') \
                                    .start()
                )
                queries.append(query)
                print(f" Stream '{name}' started -> Elasticsearch")
            except Exception as e:
                print(f" Error saving {name} to Elasticsearch: {e}")
       
        return queries
    
    
    def save_mongodb(self) :
        print('\n' + '='*60)
        print('SAVING TO MONGODB')
        print('='*60)
        def save_mg(df , batch_id , index) :
            try :
                print('\n' + '='*60)
                print(f'=== Start save {index} to MongoDB ===')
                print('='*60)

                print('=== batch id : ' , str(batch_id) , ' ===')
                
                df.write.format('mongodb') \
                                .option('database' , 'yelp_sentiment') \
                                .option('collection' , index) \
                                .mode('append') \
                                .save()
            except Exception as e:
                print("🔥 Exception inside save_es():", str(e))
                traceback.print_exc()
        queries = []
        for name,df in self.results.items() :
            try :
                query = (
                    df.writeStream.foreachBatch(lambda df , batch_id , index = name :
                                                save_mg(df , batch_id , index)) \
                                    .outputMode('append') \
                                    .start()
                )
                queries.append(query)
                print(f" Stream '{name}' started -> MongoDB")
            except Exception as e:
                print(f" Error saving {name} to MongoDB: {e}")
       
        return queries
    def save_all(self) :
        print('=== Starting all streaming jobs ===')

        queries = []
        queries += self.save_hdfs()
        queries += self.save_elasticsearch()
        # queries += self.save_mongodb()

        if queries:
            print("\n=== Waiting for streaming queries to run ===")
            print("=== ACTIVE STREAMS ===")
            for q in self.spark.streams.active:
                print(f"- {q.name}, isActive={q.isActive}, status={q.status}")
            self.spark.streams.awaitAnyTermination()
        else:
            print("\n No streaming queries started!")
            
    def generate_summary_report(self):
        """Generate summary statistics"""
        print("\n" + "="*60)
        print("SUMMARY REPORT")
        print("="*60)
        
      
        for name, df in self.results.items():
            print(f"\n{name.upper().replace('_', ' ')}:")
         
    
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

