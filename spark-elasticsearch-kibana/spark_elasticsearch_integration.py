"""
Spark-Elasticsearch-Kibana Integration System
Kết nối và trực quan hóa dữ liệu từ Spark sang Elasticsearch và Kibana
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import json
import time


# ============================================================================
# ELASTICSEARCH CONFIGURATION
# ============================================================================

class ElasticsearchConfig:
    """Cấu hình kết nối Elasticsearch"""
    
    def __init__(self, 
                 hosts=['localhost:9200'],
                 username=None,
                 password=None,
                 use_ssl=False,
                 verify_certs=False):
        """
        Khởi tạo cấu hình Elasticsearch
        
        Args:
            hosts: Danh sách Elasticsearch hosts
            username: Tên đăng nhập (nếu có)
            password: Mật khẩu (nếu có)
            use_ssl: Sử dụng SSL/TLS
            verify_certs: Xác thực chứng chỉ SSL
        """
        self.hosts = hosts
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.verify_certs = verify_certs
    
    def get_es_client(self):
        """Tạo Elasticsearch client"""
        if self.username and self.password:
            return Elasticsearch(
                self.hosts,
                basic_auth=(self.username, self.password),
                verify_certs=self.verify_certs,
                ssl_show_warn=False
            )
        else:
            return Elasticsearch(
                self.hosts,
                verify_certs=self.verify_certs
            )
    
    def get_spark_es_config(self):
        """Trả về cấu hình cho Spark-Elasticsearch connector"""
        config = {
            'es.nodes': self.hosts[0].split(':')[0],
            'es.port': self.hosts[0].split(':')[1] if ':' in self.hosts[0] else '9200',
            'es.nodes.wan.only': 'true',
            'es.index.auto.create': 'true',
            'es.mapping.id': 'business_id',  # Sử dụng business_id làm document ID
        }
        
        if self.username and self.password:
            config['es.net.http.auth.user'] = self.username
            config['es.net.http.auth.pass'] = self.password
        
        if self.use_ssl:
            config['es.net.ssl'] = 'true'
            config['es.net.ssl.cert.allow.self.signed'] = 'true'
        
        return config


# ============================================================================
# SPARK SESSION WITH ELASTICSEARCH SUPPORT
# ============================================================================

class SparkESSession:
    """Spark Session với hỗ trợ Elasticsearch"""
    
    @staticmethod
    def create_session(app_name="Spark-Elasticsearch Integration"):
        """
        Tạo Spark Session với Elasticsearch connector
        
        Lưu ý: Cần có elasticsearch-spark jar trong classpath
        Download từ: https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/
        """
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", 
                    "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark


# ============================================================================
# DATA TRANSFER TO ELASTICSEARCH
# ============================================================================

class SparkToElasticsearch:
    """Chuyển dữ liệu từ Spark DataFrame sang Elasticsearch"""
    
    def __init__(self, es_config):
        """
        Args:
            es_config: ElasticsearchConfig instance
        """
        self.es_config = es_config
        self.es_client = es_config.get_es_client()
    
    def create_index_with_mapping(self, index_name, mapping):
        """
        Tạo index với mapping cụ thể
        
        Args:
            index_name: Tên index
            mapping: Dictionary chứa mapping definition
        """
        try:
            if self.es_client.indices.exists(index=index_name):
                print(f"Index '{index_name}' đã tồn tại. Xóa và tạo lại...")
                self.es_client.indices.delete(index=index_name)
            
            self.es_client.indices.create(index=index_name, body=mapping)
            print(f"✓ Đã tạo index '{index_name}' thành công")
            
        except Exception as e:
            print(f"✗ Lỗi khi tạo index '{index_name}': {str(e)}")
            raise
    
    def write_dataframe_to_es(self, df, index_name, mode='append'):
        """
        Ghi Spark DataFrame vào Elasticsearch sử dụng Spark connector
        
        Args:
            df: Spark DataFrame
            index_name: Tên index trong Elasticsearch
            mode: 'append' hoặc 'overwrite'
        """
        try:
            print(f"\nĐang ghi dữ liệu vào index '{index_name}'...")
            start_time = time.time()
            
            # Lấy cấu hình Elasticsearch
            es_conf = self.es_config.get_spark_es_config()
            
            # Ghi dữ liệu
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .options(**es_conf) \
                .option("es.resource", index_name) \
                .mode(mode) \
                .save()
            
            elapsed = time.time() - start_time
            count = df.count()
            print(f"✓ Đã ghi {count:,} documents vào '{index_name}' trong {elapsed:.2f}s")
            
        except Exception as e:
            print(f"✗ Lỗi khi ghi dữ liệu vào '{index_name}': {str(e)}")
            raise
    
    def bulk_insert_from_pandas(self, pandas_df, index_name, id_field=None):
        """
        Bulk insert từ Pandas DataFrame (cho dữ liệu nhỏ)
        
        Args:
            pandas_df: Pandas DataFrame
            index_name: Tên index
            id_field: Trường sử dụng làm document ID
        """
        try:
            print(f"\nĐang bulk insert vào '{index_name}'...")
            
            actions = []
            for idx, row in pandas_df.iterrows():
                doc = row.to_dict()
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                
                actions.append(action)
            
            # Bulk insert
            success, failed = helpers.bulk(
                self.es_client,
                actions,
                raise_on_error=False,
                stats_only=False
            )
            
            print(f"✓ Bulk insert hoàn tất: {success} thành công, {len(failed)} thất bại")
            
        except Exception as e:
            print(f"✗ Lỗi bulk insert: {str(e)}")
            raise


# ============================================================================
# INDEX MAPPINGS FOR EACH ANALYSIS
# ============================================================================

class ElasticsearchMappings:
    """Định nghĩa mapping cho các index"""
    
    @staticmethod
    def top_selling_mapping():
        """Mapping cho analysis 1: Top selling products"""
        return {
            "mappings": {
                "properties": {
                    "business_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "city": {"type": "keyword"},
                    "state": {"type": "keyword"},
                    "categories": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "recent_review_count": {"type": "integer"},
                    "avg_rating": {"type": "float"},
                    "timestamp": {"type": "date"}
                }
            }
        }
    
    @staticmethod
    def diverse_stores_mapping():
        """Mapping cho analysis 2: Diverse stores"""
        return {
            "mappings": {
                "properties": {
                    "business_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "city": {"type": "keyword"},
                    "state": {"type": "keyword"},
                    "categories": {"type": "text"},
                    "category_count": {"type": "integer"},
                    "review_count": {"type": "integer"},
                    "stars": {"type": "float"},
                    "timestamp": {"type": "date"}
                }
            }
        }
    
    @staticmethod
    def best_rated_mapping():
        """Mapping cho analysis 3: Best rated products"""
        return {
            "mappings": {
                "properties": {
                    "business_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "city": {"type": "keyword"},
                    "state": {"type": "keyword"},
                    "categories": {"type": "text"},
                    "total_reviews": {"type": "integer"},
                    "avg_review_stars": {"type": "float"},
                    "total_useful": {"type": "integer"},
                    "business_avg_stars": {"type": "float"},
                    "timestamp": {"type": "date"}
                }
            }
        }
    
    @staticmethod
    def positive_reviews_mapping():
        """Mapping cho analysis 4: Most positive reviews"""
        return {
            "mappings": {
                "properties": {
                    "business_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "city": {"type": "keyword"},
                    "state": {"type": "keyword"},
                    "categories": {"type": "text"},
                    "positive_review_count": {"type": "integer"},
                    "total_review_count": {"type": "integer"},
                    "positive_ratio": {"type": "float"},
                    "avg_positive_rating": {"type": "float"},
                    "total_useful_votes": {"type": "integer"},
                    "timestamp": {"type": "date"}
                }
            }
        }
    
    @staticmethod
    def peak_hours_mapping():
        """Mapping cho analysis 5: Peak hours"""
        return {
            "mappings": {
                "properties": {
                    "year": {"type": "integer"},
                    "month": {"type": "integer"},
                    "review_count": {"type": "integer"},
                    "date_string": {"type": "keyword"},
                    "timestamp": {"type": "date"}
                }
            }
        }
    
    @staticmethod
    def top_categories_mapping():
        """Mapping cho analysis 6: Top categories"""
        return {
            "mappings": {
                "properties": {
                    "category": {"type": "keyword"},
                    "total_reviews": {"type": "integer"},
                    "timestamp": {"type": "date"}
                }
            }
        }
    
    @staticmethod
    def store_stats_mapping():
        """Mapping cho analysis 7: Store statistics"""
        return {
            "mappings": {
                "properties": {
                    "business_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "city": {"type": "keyword"},
                    "state": {"type": "keyword"},
                    "categories": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "stars": {"type": "float"},
                    "review_count": {"type": "integer"},
                    "actual_review_count": {"type": "integer"},
                    "actual_avg_stars": {"type": "float"},
                    "location": {"type": "geo_point"},
                    "timestamp": {"type": "date"}
                }
            }
        }


# ============================================================================
# MAIN INTEGRATION PIPELINE
# ============================================================================

class YelpElasticsearchPipeline:
    """Pipeline tích hợp Yelp Analysis với Elasticsearch"""
    
    def __init__(self, spark, es_config, yelp_pipeline):
        """
        Args:
            spark: Spark Session
            es_config: ElasticsearchConfig instance
            yelp_pipeline: YelpAnalysisPipeline instance (từ file gốc)
        """
        self.spark = spark
        self.es_config = es_config
        self.yelp_pipeline = yelp_pipeline
        self.es_writer = SparkToElasticsearch(es_config)
        self.mappings = ElasticsearchMappings()
    
    def prepare_dataframe_for_es(self, df, analysis_type):
        """
        Chuẩn bị DataFrame trước khi ghi vào ES
        - Thêm timestamp
        - Xử lý null values
        - Tạo composite IDs nếu cần
        """
        # Thêm timestamp
        df = df.withColumn("timestamp", lit(datetime.now().isoformat()))
        
        # Xử lý các trường hợp đặc biệt cho từng loại analysis
        if analysis_type == "peak_hours":
            # Tạo ID duy nhất từ year-month
            df = df.withColumn("_id", 
                              concat(col("year"), lit("-"), col("month")))
            df = df.withColumn("date_string",
                              concat(col("year"), lit("-"), 
                                    lpad(col("month"), 2, "0")))
        
        elif analysis_type == "top_categories":
            # Tạo ID từ category name
            df = df.withColumn("_id", col("category"))
        
        return df
    
    def export_all_analyses(self):
        """Export tất cả các phân tích vào Elasticsearch"""
        
        print("\n" + "="*80)
        print(" "*20 + "EXPORTING TO ELASTICSEARCH")
        print("="*80)
        
        analyses = [
            {
                'name': 'Analysis 1: Top Selling Products',
                'index': 'yelp-top-selling',
                'result_key': 'top_selling',
                'mapping': self.mappings.top_selling_mapping(),
                'type': 'top_selling'
            },
            {
                'name': 'Analysis 2: Diverse Stores',
                'index': 'yelp-diverse-stores',
                'result_key': 'diverse_stores',
                'mapping': self.mappings.diverse_stores_mapping(),
                'type': 'diverse_stores'
            },
            {
                'name': 'Analysis 3: Best Rated Products',
                'index': 'yelp-best-rated',
                'result_key': 'best_rated',
                'mapping': self.mappings.best_rated_mapping(),
                'type': 'best_rated'
            },
            {
                'name': 'Analysis 4: Most Positive Reviews',
                'index': 'yelp-positive-reviews',
                'result_key': 'most_positive',
                'mapping': self.mappings.positive_reviews_mapping(),
                'type': 'positive_reviews'
            },
            {
                'name': 'Analysis 5: Peak Hours',
                'index': 'yelp-peak-hours',
                'result_key': 'peak_hours',
                'mapping': self.mappings.peak_hours_mapping(),
                'type': 'peak_hours'
            },
            {
                'name': 'Analysis 6: Top Categories',
                'index': 'yelp-top-categories',
                'result_key': 'top_categories',
                'mapping': self.mappings.top_categories_mapping(),
                'type': 'top_categories'
            },
            {
                'name': 'Analysis 7: Store Statistics',
                'index': 'yelp-store-stats',
                'result_key': 'store_stats',
                'mapping': self.mappings.store_stats_mapping(),
                'type': 'store_stats'
            }
        ]
        
        for analysis in analyses:
            try:
                print(f"\n{'='*60}")
                print(f"Processing: {analysis['name']}")
                print(f"{'='*60}")
                
                # Kiểm tra xem kết quả có tồn tại không
                if analysis['result_key'] not in self.yelp_pipeline.results:
                    print(f"⚠ Bỏ qua - Chưa có kết quả cho {analysis['name']}")
                    continue
                
                # Tạo index với mapping
                self.es_writer.create_index_with_mapping(
                    analysis['index'], 
                    analysis['mapping']
                )
                
                # Lấy DataFrame kết quả
                df = self.yelp_pipeline.results[analysis['result_key']]
                
                # Chuẩn bị DataFrame
                df_prepared = self.prepare_dataframe_for_es(df, analysis['type'])
                
                # Ghi vào Elasticsearch
                self.es_writer.write_dataframe_to_es(
                    df_prepared, 
                    analysis['index'],
                    mode='overwrite'
                )
                
                print(f"✓ {analysis['name']} đã được export thành công!")
                
            except Exception as e:
                print(f"✗ Lỗi khi export {analysis['name']}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    def create_kibana_index_patterns(self):
        """
        Tạo index patterns trong Kibana
        Lưu ý: Cần chạy trong Kibana hoặc qua API
        """
        index_patterns = [
            "yelp-top-selling",
            "yelp-diverse-stores",
            "yelp-best-rated",
            "yelp-positive-reviews",
            "yelp-peak-hours",
            "yelp-top-categories",
            "yelp-store-stats"
        ]
        
        print("\n" + "="*80)
        print("KIBANA INDEX PATTERNS")
        print("="*80)
        print("\nĐể tạo index patterns trong Kibana:")
        print("1. Mở Kibana: http://localhost:5601")
        print("2. Vào Stack Management > Index Patterns")
        print("3. Click 'Create index pattern'")
        print("4. Tạo các index pattern sau:\n")
        
        for pattern in index_patterns:
            print(f"   - {pattern}")
            print(f"     Time field: timestamp\n")


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

def main():
    """
    Ví dụ sử dụng pipeline integration
    """
    
    print("\n" + "="*80)
    print(" "*15 + "YELP SPARK-ELASTICSEARCH INTEGRATION")
    print("="*80)
    
    # 1. Tạo Spark Session
    spark = SparkESSession.create_session()
    
    # 2. Cấu hình Elasticsearch
    es_config = ElasticsearchConfig(
        hosts=['localhost:9200'],
        # username='elastic',  # Uncomment nếu có authentication
        # password='your_password',
        use_ssl=False
    )
    
    # 3. Kiểm tra kết nối Elasticsearch
    try:
        es_client = es_config.get_es_client()
        info = es_client.info()
        print(f"\n✓ Đã kết nối Elasticsearch: {info['version']['number']}")
    except Exception as e:
        print(f"\n✗ Không thể kết nối Elasticsearch: {str(e)}")
        return
    
    # 4. Import YelpAnalysisPipeline từ file gốc
    # Giả sử file gốc đã được import
    from spark_yelp_analysis import YelpAnalysisPipeline
    
    # 5. Chạy Yelp Analysis Pipeline
    yelp_pipeline = YelpAnalysisPipeline(
        data_path="../data/",
        output_path="output/"
    )
    
    try:
        # Load data
        yelp_pipeline.load_data()
        
        # Chạy tất cả analyses
        yelp_pipeline.run_all_analyses(config={
            'analysis_1': {'days': 90, 'top_n': 10},
            'analysis_2': {'top_n': 10},
            'analysis_3': {'min_reviews': 50, 'top_n': 10},
            'analysis_4': {'positive_threshold': 4, 'top_n': 10},
            'analysis_6': {'top_n': 20}
        })
        
        # 6. Export to Elasticsearch
        es_pipeline = YelpElasticsearchPipeline(spark, es_config, yelp_pipeline)
        es_pipeline.export_all_analyses()
        
        # 7. Hiển thị hướng dẫn tạo Kibana index patterns
        es_pipeline.create_kibana_index_patterns()
        
        print("\n" + "="*80)
        print(" "*25 + "HOÀN TẤT!")
        print("="*80)
        print("\nDữ liệu đã được gửi sang Elasticsearch.")
        print("Bây giờ bạn có thể:")
        print("1. Mở Kibana tại http://localhost:5601")
        print("2. Tạo các Index Patterns")
        print("3. Tạo Visualizations và Dashboards")
        
    except Exception as e:
        print(f"\n✗ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
