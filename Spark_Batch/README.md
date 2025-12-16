# 🚀 YELP BIG DATA ANALYSIS - BATCH MODE

Hệ thống phân tích dữ liệu Yelp sử dụng PySpark với các tính năng Spark nâng cao.

---

## ⚡ QUICK START

### 1. Cài đặt dependencies
```bash
pip3 install pyspark pandas pyarrow
```

### 2. Chạy pipeline
```bash
# Chạy phiên bản đầy đủ (9 analyses)
python3 batch_main_v2.py --data-path ./data/

# Hoặc chạy phiên bản cơ bản (7 analyses)
python3 batch_main.py --data-path ./data/
```

### 3. Test các tính năng
```bash
# Test tất cả tính năng nâng cao
python3 test_local_features.py --test all

# Test từng feature riêng
python3 test_local_features.py --test udf
python3 test_local_features.py --test window
python3 test_local_features.py --test pivot
python3 test_local_features.py --test broadcast
```

---

## 📁 CẤU TRÚC NGẮN GỌN

```
Spark_Batch/
│
├── 🎯 Entry Points
│   ├── batch_main.py         → Chạy 7 analyses cơ bản
│   └── batch_main_v2.py      → Chạy 9 analyses nâng cao ⭐
│
├── 🔧 Core Modules
│   ├── batch_configuration.py → Spark config & schemas
│   ├── batch_load_data.py     → Load dữ liệu từ JSON
│   └── batch_pipeline.py      → Pipeline orchestrator
│
├── 📊 Analytics
│   ├── batch_analytics.py            → 7 analyses cơ bản
│   └── batch_analytics_advanced.py   → 2 analyses nâng cao ⭐
│
├── 🎯 UDF Library
│   └── batch_udf.py           → 7 UDFs (3 Regular + 4 Pandas) ⭐
│
└── 🧪 Testing
    └── test_local_features.py → Test suite ⭐
```

⭐ = Files mới trong Phase 1 (Advanced Features)

---

## 🎓 CÁC PHÂN TÍCH

### V1 - 7 Analyses Cơ bản

| ID | Tên Analysis | Tính năng |
|---|---|---|
| 1 | Top Selling Products | Broadcast Join |
| 2 | User Purchase Patterns | Aggregation |
| 3 | Top Users by Reviews | Broadcast Join |
| 4 | Category Trends Over Time | Broadcast Join |
| 5 | High Rating Low Review Count | Filter & Aggregation |
| 6 | Geographic Distribution | Broadcast Join |
| 7 | Seasonal Trends | Time-based Analysis |

### V2 - 2 Analyses Nâng cao (Mới!)

| ID | Tên Analysis | Tính năng |
|---|---|---|
| 8 | Trending Businesses | **Window Functions** (lag, lead, rank, avg) |
| 9 | Category Performance Matrix | **Pivot/Unpivot** Operations |

---

## 🔥 TÍNH NĂNG NỔI BẬT

### 1. UDF Library (`batch_udf.py`)
- **3 Regular UDFs**: categorize_rating, is_weekend, extract_city_state
- **4 Pandas UDFs**: sentiment_score, extract_keywords, text_length_normalized, extract_hashtags
- **Performance**: Pandas UDF nhanh hơn 10-100x

### 2. Window Functions (Analysis 8)
- `lag()`, `lead()` - So sánh time series
- `avg() over window` - Moving averages
- `dense_rank()` - Ranking
- `rowsBetween()` - Sliding windows

### 3. Pivot/Unpivot (Analysis 9)
- `pivot()` - Long → Wide format
- `stack()` - Wide → Long format
- Cross-tabulation analysis

### 4. Broadcast Join Optimization
- Optimized cho join với small tables
- Áp dụng cho analyses 1, 3, 4, 6

---

## 📊 KẾT QUẢ

### Spark Skills Coverage
- **Trước**: 42%
- **Sau**: 64%
- **Cải thiện**: +22%

### Code Statistics
- **Tổng dòng code**: 2,712 lines
- **Số analyses**: 9
- **Số UDFs**: 7
- **Test coverage**: 4 feature tests

---

## 📖 TÀI LIỆU CHI TIẾT

| Tài liệu | Nội dung |
|---|---|
| **PROJECT_STRUCTURE.md** | Cấu trúc chi tiết toàn bộ dự án (ĐỌCNÀY!) |
| **LOCAL_TEST_GUIDE.md** | Hướng dẫn test từng bước |
| **IMPLEMENTATION_PLAN_PA1.md** | Kế hoạch triển khai Phase 1 |
| **QUICKSTART.md** | Hướng dẫn nhanh |

---

## 🎯 SỬ DỤNG TỪNG MODULE

### Chỉ cần Spark Session?
```python
from batch_configuration import SparkConfig
spark = SparkConfig.create_spark_session()
```

### Chỉ cần load dữ liệu?
```python
from batch_load_data import DataLoader
loader = DataLoader(spark, "./data/")
business_df = loader.load_business_data()
review_df = loader.load_review_data()
```

### Chỉ chạy 1 analysis?
```python
from batch_analytics import YelpAnalytics
analytics = YelpAnalytics()
result = analytics.top_selling_products_recent(review_df, business_df)
result.show()
```

### Sử dụng UDF?
```python
from batch_udf import sentiment_score, categorize_rating
from pyspark.sql.functions import col

df = df.withColumn("sentiment", sentiment_score(col("text")))
df = df.withColumn("rating_label", categorize_rating(col("stars")))
```

### Chạy analysis nâng cao?
```python
from batch_analytics_advanced import AdvancedYelpAnalytics
advanced = AdvancedYelpAnalytics()
result = advanced.trending_businesses(review_df, business_df, window_days=90)
result.show()
```

---

## 🐛 TROUBLESHOOTING

### ImportError: No module named 'pyspark'
```bash
pip3 install pyspark==3.4.1
```

### ImportError: No module named 'pandas'
```bash
pip3 install pandas==2.0.3 pyarrow==12.0.0
```

### OutOfMemoryError
Giảm memory trong `batch_configuration.py`:
```python
.config("spark.driver.memory", "4g")  # thay vì 8g
.config("spark.executor.memory", "2g")  # thay vì 4g
```

### Broadcast join không hoạt động
Check physical plan:
```python
result.explain()
# Phải thấy "BroadcastHashJoin" hoặc "BroadcastExchange"
```

---

## 💡 TIPS & BEST PRACTICES

### Performance
- ✅ Sử dụng **Pandas UDF** thay vì Regular UDF khi có thể
- ✅ Sử dụng **broadcast()** cho joins với bảng nhỏ (<10MB)
- ✅ **Cache** DataFrame nếu sử dụng nhiều lần
- ✅ Sử dụng **explicit schemas** khi load data

### Code Organization
- ✅ Mỗi analysis là một static method riêng biệt
- ✅ Tách configuration, data loading, và analytics logic
- ✅ Error handling trong mỗi analysis function
- ✅ Modularity - mỗi module có thể chạy độc lập

### Testing
- ✅ Test từng feature trước khi integration
- ✅ Sử dụng sample data nhỏ khi develop
- ✅ Check physical plan với `.explain()` để verify optimizations

---

## 🚀 ROADMAP

### ✅ Phase 1 (Completed)
- [x] UDF Library (7 UDFs)
- [x] Window Functions (Analysis 8)
- [x] Pivot/Unpivot (Analysis 9)
- [x] Broadcast Join Optimization
- [x] Test Suite
- [x] Documentation

### 🔜 Phase 2 (Future)
- [ ] Machine Learning Pipeline
- [ ] Graph Processing (GraphFrames)
- [ ] Advanced Aggregations (ROLLUP, CUBE)
- [ ] Performance tuning với AQE

---

## 📞 HỖ TRỢ

### Cần hiểu cấu trúc chi tiết?
👉 Đọc **PROJECT_STRUCTURE.md** (80KB, rất chi tiết!)

### Cần test local?
👉 Đọc **LOCAL_TEST_GUIDE.md**

### Cần chạy nhanh?
👉 Xem ⬆️ phần QUICK START ở trên

---

## 📝 VERSION HISTORY

| Version | Date | Changes |
|---|---|---|
| 2.0 | 2025-12-16 | ✨ Add advanced features (UDF, Window, Pivot, Broadcast) |
| 1.0 | 2025-12-15 | 🎉 Initial batch implementation (7 analyses) |

---

**Current Version**: 2.0 (Advanced Features)
**Last Updated**: 2025-12-16
**Branch**: `claude/review-project-structure-pfDJE`

---

## 🎉 BẮT ĐẦU NGAY!

```bash
# 1. Clone repo (nếu chưa có)
git clone <repo-url>
cd bigdata-2025-1/Spark_Batch

# 2. Cài đặt dependencies
pip3 install pyspark pandas pyarrow

# 3. Test features
python3 test_local_features.py --test all

# 4. Chạy pipeline
python3 batch_main_v2.py --data-path ./data/

# 5. Xem kết quả
ls -lh output_v2/
```

---

**Happy Analyzing! 🎊**
