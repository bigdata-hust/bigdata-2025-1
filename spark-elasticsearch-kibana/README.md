# 🚀 Hệ thống Spark-Elasticsearch-Kibana cho Yelp Data

## 📁 Cấu trúc thư mục yêu cầu

```
your-project/
├── processed_data/              ← DỮ LIỆU CỦA BẠN (thư mục này cần tồn tại)
│   ├── business.csv
│   ├── user.csv
│   └── review_combined_1.csv
│
└── spark-elasticsearch-kibana/  ← CODE (sau khi giải nén zip)
    ├── main.py                  ⭐ FILE CHẠY CHÍNH
    ├── csv_data_loader.py       📊 Load CSV data
    ├── yelp_analytics.py        📈 Các hàm phân tích
    ├── spark_elasticsearch_integration.py  🔗 Kết nối ES
    ├── docker-compose.yml       🐳 ES & Kibana setup
    ├── requirements.txt         📦 Dependencies
    ├── env.example              ⚙️ Cấu hình mẫu
    └── README.md               📖 File này
```

**⚠️ QUAN TRỌNG**: Thư mục `processed_data` phải nằm **NGANG HÀNG** với thư mục `spark-elasticsearch-kibana`

---

## 🎯 Chức năng chính

### ✅ Hệ thống này sẽ:

1. **Đọc dữ liệu CSV** từ thư mục `processed_data`
2. **Phân tích 7 loại insights** từ Yelp data:
   - Top sản phẩm bán chạy
   - Cửa hàng đa dạng nhất
   - Đánh giá tốt nhất
   - Review tích cực nhất
   - Thời gian cao điểm
   - Top categories
   - Thống kê tổng hợp

3. **Export kết quả** vào Elasticsearch (7 indices)
4. **Trực quan hóa** trên Kibana dashboards

---

## ⚡ Quick Start (5 phút)

### Bước 1: Chuẩn bị môi trường

```bash
# Di chuyển vào thư mục code
cd spark-elasticsearch-kibana

# Cài đặt Python packages
pip install -r requirements.txt
```

### Bước 2: Khởi động Elasticsearch & Kibana

```bash
# Khởi động Docker containers
docker-compose up -d

# Đợi ~30 giây để services sẵn sàng

# Kiểm tra trạng thái
docker-compose ps

# Test kết nối
curl http://localhost:9200
```

**URLs sau khi khởi động:**
- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601

### Bước 3: Kiểm tra dữ liệu

```bash
# Đảm bảo các file CSV tồn tại
ls -lh ../processed_data/

# Output mong đợi:
# business.csv
# user.csv
# review_combined_1.csv
```

### Bước 4: Chạy Pipeline

```bash
# Chạy file chính
python main.py
```

Pipeline sẽ tự động:
- ✅ Load CSV data
- ✅ Validate dữ liệu
- ✅ Chạy 7 analyses
- ✅ Export sang Elasticsearch
- ✅ Hiển thị kết quả

**Thời gian dự kiến**: 5-15 phút (tùy kích thước data)

### Bước 5: Tạo Dashboard trong Kibana

1. Mở browser: http://localhost:5601
2. Vào **Stack Management** → **Index Patterns**
3. Click **Create index pattern**
4. Tạo pattern: `yelp-top-selling*` (time field: `timestamp`)
5. Lặp lại cho 6 indices còn lại
6. Vào **Visualize** → Tạo visualizations
7. Vào **Dashboard** → Tạo dashboard

---

## 📊 Các Index được tạo

| Index Name | Mô tả | Documents |
|-----------|-------|-----------|
| `yelp-top-selling` | Top 10 sản phẩm bán chạy gần đây | ~10 |
| `yelp-diverse-stores` | Top 10 cửa hàng đa dạng | ~10 |
| `yelp-best-rated` | Top 10 đánh giá tốt nhất | ~10 |
| `yelp-positive-reviews` | Top 10 review tích cực | ~10 |
| `yelp-peak-hours` | Thống kê theo thời gian | Variable |
| `yelp-top-categories` | Top 20 categories | ~20 |
| `yelp-store-stats` | Tất cả businesses | All |

---

## 🔧 Cấu hình

### Environment Variables (Optional)

```bash
# Copy file mẫu
cp env.example .env

# Edit cấu hình
nano .env
```

**Các biến quan trọng:**
```bash
# Elasticsearch
ES_HOST=localhost
ES_PORT=9200

# Data path
DATA_PATH=../processed_data/

# Spark resources
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
```

### Điều chỉnh Memory (nếu cần)

**Docker (cho Elasticsearch):**
```yaml
# docker-compose.yml
elasticsearch:
  environment:
    - "ES_JAVA_OPTS=-Xms2g -Xmx2g"  # Giảm nếu máy yếu
```

**Spark:**
```python
# Trong main.py, tìm SparkESSession.create_session()
# Thêm configs:
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "8g")
```

---

## 🎨 Ví dụ Visualizations trong Kibana

### 1. Top Selling Products (Data Table)
```
Index: yelp-top-selling
Columns: name, city, recent_review_count, avg_rating
Sort: recent_review_count DESC
```

### 2. Review Trends (Line Chart)
```
Index: yelp-peak-hours
X-axis: date_string (Date)
Y-axis: review_count (Sum)
```

### 3. Top Categories (Bar Chart)
```
Index: yelp-top-categories
X-axis: category
Y-axis: total_reviews
Top: 15 categories
```

### 4. Geographic Distribution (Map)
```
Index: yelp-store-stats
Geo field: location (latitude, longitude)
Size by: actual_review_count
Color by: actual_avg_stars
```

---

## 🆘 Troubleshooting

### Lỗi 1: "FileNotFoundError: ../processed_data/business.csv"

**Nguyên nhân**: Thư mục data không đúng vị trí

**Giải pháp**:
```bash
# Kiểm tra cấu trúc thư mục
pwd
ls ..

# Nên thấy:
# processed_data/
# spark-elasticsearch-kibana/

# Nếu không đúng, di chuyển thư mục hoặc cập nhật DATA_PATH
```

### Lỗi 2: "Connection refused to Elasticsearch"

**Nguyên nhân**: Elasticsearch chưa chạy

**Giải pháp**:
```bash
docker-compose up -d
docker-compose ps  # Kiểm tra status

# Nếu vẫn lỗi
docker-compose logs elasticsearch
```

### Lỗi 3: "Spark out of memory"

**Giải pháp**:
```bash
# Giảm partitions trong main.py
# Tìm dòng: .repartition(200, ...)
# Đổi thành: .repartition(50, ...)

# Hoặc tăng Spark memory
# Edit main.py, thêm configs
```

### Lỗi 4: "Schema mismatch in CSV"

**Nguyên nhân**: CSV format không đúng

**Giải pháp**:
```python
# Kiểm tra CSV headers
head -1 ../processed_data/business.csv

# Đảm bảo có các cột:
# business_id, name, city, state, categories, stars, review_count, is_open, latitude, longitude
```

### Lỗi 5: "Date parsing failed"

**Giải pháp**: Đã có auto-detect format trong code, nhưng nếu vẫn lỗi:
```python
# Edit csv_data_loader.py
# Thêm format date của bạn vào coalesce()
```

---

## 📈 Performance Tips

### Tăng tốc Pipeline:

1. **Reduce data scope**
```python
# Trong main.py, thêm filter
review_df = review_df.filter(col("review_date") >= "2023-01-01")
```

2. **Adjust partitions**
```python
# Giảm cho small data
.repartition(50, "business_id")

# Tăng cho big data
.repartition(400, "business_id")
```

3. **Cache frequently used data**
```python
# Đã có trong code, nhưng có thể thêm:
business_df.cache()
business_df.count()  # Trigger cache
```

4. **Limit results for testing**
```python
# Trong analysis_config
'analysis_1': {'days': 30, 'top_n': 5}  # Giảm từ 90 days, 10 results
```

---

## 🎓 Project Structure Chi tiết

```
spark-elasticsearch-kibana/
│
├── main.py                              ⭐ ENTRY POINT
│   ├── Khởi tạo Spark
│   ├── Cấu hình Elasticsearch
│   ├── Load CSV data
│   ├── Chạy analyses
│   ├── Export to ES
│   └── Hiển thị kết quả
│
├── csv_data_loader.py                   📊 DATA LOADING
│   ├── CSVDataLoader
│   │   ├── load_business_data()
│   │   ├── load_review_data()
│   │   ├── load_user_data()
│   │   └── validate_data()
│   └── YelpAnalyticsPipeline
│       ├── load_all_data()
│       └── get_dataframes()
│
├── yelp_analytics.py                    📈 ANALYTICS
│   └── YelpAnalytics
│       ├── top_selling_products_recent()
│       ├── top_stores_by_product_count()
│       ├── top_rated_products()
│       ├── top_stores_by_positive_reviews()
│       ├── get_peak_hours()
│       ├── get_top_categories()
│       └── get_store_stats()
│
├── spark_elasticsearch_integration.py   🔗 ES INTEGRATION
│   ├── ElasticsearchConfig
│   ├── SparkESSession
│   ├── SparkToElasticsearch
│   ├── ElasticsearchMappings
│   └── YelpElasticsearchPipeline
│
├── docker-compose.yml                   🐳 SERVICES
│   ├── Elasticsearch 8.11.0
│   └── Kibana 8.11.0
│
├── requirements.txt                     📦 DEPENDENCIES
│   ├── pyspark>=3.4.0
│   ├── elasticsearch>=8.11.0
│   └── pandas>=1.5.0
│
└── env.example                          ⚙️ CONFIG TEMPLATE
```

---

## 🔄 Workflow

```
CSV FILES (processed_data/)
    ↓
LOAD & VALIDATE (csv_data_loader.py)
    ↓
ANALYZE (yelp_analytics.py)
    ├── Analysis 1: Top Selling
    ├── Analysis 2: Diverse Stores
    ├── Analysis 3: Best Rated
    ├── Analysis 4: Positive Reviews
    ├── Analysis 5: Peak Hours
    ├── Analysis 6: Top Categories
    └── Analysis 7: Store Stats
    ↓
TRANSFORM & EXPORT (spark_elasticsearch_integration.py)
    ↓
ELASTICSEARCH (7 indices)
    ↓
KIBANA (Visualizations & Dashboards)
```

---

## ✅ Verification Checklist

Sau khi chạy xong, kiểm tra:

- [ ] Thư mục `processed_data` đúng vị trí
- [ ] 3 file CSV tồn tại và có data
- [ ] Docker containers đang chạy
- [ ] Elasticsearch accessible (curl localhost:9200)
- [ ] Kibana accessible (curl localhost:5601)
- [ ] Pipeline chạy không lỗi
- [ ] 7 indices đã được tạo
- [ ] Có thể query data từ ES
- [ ] Index patterns tạo được trong Kibana
- [ ] Data hiển thị trong Discover

**Kiểm tra indices:**
```bash
curl http://localhost:9200/_cat/indices?v | grep yelp
```

**Đếm documents:**
```bash
curl http://localhost:9200/yelp-top-selling/_count
```

---

## 🎯 Next Steps

### 1. Tùy chỉnh Analyses
```python
# Edit main.py, section analysis_config
analysis_config = {
    'analysis_1': {'days': 180, 'top_n': 20},  # 6 tháng, top 20
    ...
}
```

### 2. Thêm Analyses mới
```python
# Trong yelp_analytics.py
@staticmethod
def custom_analysis(business_df, review_df):
    # Your analysis logic
    return result_df

# Trong main.py, thêm vào run_analysis()
results['custom'] = analytics.custom_analysis(business_df, review_df)
```

### 3. Schedule Pipeline
```bash
# Crontab (Linux)
0 2 * * * cd /path/to/spark-elasticsearch-kibana && python main.py

# Task Scheduler (Windows)
# Tạo task chạy main.py hàng ngày
```

### 4. Export to other formats
```python
# Thêm vào main.py sau analyses
results['top_selling'].write.csv("output/top_selling.csv")
results['top_selling'].write.parquet("output/top_selling.parquet")
```

---

## 📚 Tài nguyên bổ sung

- **Elasticsearch Guide**: https://www.elastic.co/guide/en/elasticsearch/reference/current/
- **Kibana Guide**: https://www.elastic.co/guide/en/kibana/current/
- **PySpark Docs**: https://spark.apache.org/docs/latest/api/python/
- **ES-Hadoop**: https://www.elastic.co/guide/en/elasticsearch/hadoop/current/

---

## 🆘 Support

Nếu gặp vấn đề:

1. **Check logs**
```bash
# Elasticsearch logs
docker-compose logs elasticsearch

# Kibana logs
docker-compose logs kibana

# Python errors
# Xem trong terminal output
```

2. **Common commands**
```bash
# Restart services
docker-compose restart

# Stop services
docker-compose down

# View all containers
docker ps -a

# Check Spark UI
# http://localhost:4040 (khi pipeline đang chạy)
```

3. **Debug mode**
```python
# Thêm vào đầu main.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📝 Notes

- **Data Format**: CSV với header, encoding UTF-8
- **Spark Version**: 3.4.0+
- **Elasticsearch**: 8.11.0
- **Python**: 3.8+
- **Memory**: Khuyến nghị 8GB+ RAM
- **Disk**: ~2GB cho Docker images

---

## 🎉 Kết luận

Bạn đã có một hệ thống hoàn chỉnh để:

✅ Load dữ liệu Yelp từ CSV  
✅ Phân tích insights tự động  
✅ Export sang Elasticsearch  
✅ Visualize trên Kibana  
✅ Scale với big data  

**Chúc bạn thành công! 🚀📊**

---

*Version: 1.0.0*  
*Last Updated: 2025-11-02*
