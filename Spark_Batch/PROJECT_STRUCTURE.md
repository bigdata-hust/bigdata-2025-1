# 📁 CẤU TRÚC DỰ ÁN - YELP BIG DATA ANALYSIS (BATCH MODE)

## 📊 Tổng quan

Dự án phân tích dữ liệu Yelp sử dụng PySpark với 2 phiên bản:
- **V1 (Basic)**: 7 analyses cơ bản
- **V2 (Advanced)**: 9 analyses với các tính năng Spark nâng cao

---

## 🗂️ Cấu trúc thư mục

```
Spark_Batch/
│
├── 📋 ENTRY POINTS (Điểm khởi chạy)
│   ├── batch_main.py              # Main V1 - Chạy 7 analyses cơ bản
│   └── batch_main_v2.py           # Main V2 - Chạy 9 analyses với tính năng nâng cao
│
├── 🔧 CORE MODULES (Module cốt lõi)
│   ├── batch_configuration.py    # Cấu hình Spark Session & schemas
│   ├── batch_load_data.py        # Load dữ liệu từ JSON
│   └── batch_pipeline.py         # Pipeline orchestrator chính
│
├── 📊 ANALYTICS MODULES (Module phân tích)
│   ├── batch_analytics.py        # 7 analyses cơ bản (V1)
│   └── batch_analytics_advanced.py # 2 analyses nâng cao (V2)
│
├── 🎯 UDF LIBRARY (Thư viện hàm tùy chỉnh)
│   └── batch_udf.py              # 7 UDFs (3 Regular + 4 Pandas UDF)
│
├── 🧪 TESTING (Kiểm thử)
│   └── test_local_features.py    # Test suite cho 4 tính năng nâng cao
│
├── 🛠️ UTILITIES (Tiện ích)
│   └── create_sample_data.py     # Tạo dữ liệu mẫu để test
│
└── 📚 DOCUMENTATION (Tài liệu)
    ├── README_BATCH.md           # Hướng dẫn cơ bản
    ├── QUICKSTART.md             # Quick start guide
    ├── LOCAL_TEST_GUIDE.md       # Hướng dẫn test local chi tiết
    └── IMPLEMENTATION_PLAN_PA1.md # Kế hoạch triển khai Phase 1
```

---

## 🔍 CHI TIẾT TỪNG MODULE

### 1️⃣ **ENTRY POINTS** - Điểm khởi chạy

#### `batch_main.py` (185 dòng)
**Chức năng**: Entry point cho phiên bản V1 cơ bản

**Các hàm chính**:
- `main()` - Hàm chính khởi chạy pipeline
- `print_header()` - In header thông tin
- `print_footer()` - In footer kết quả

**Cách sử dụng**:
```bash
python batch_main.py --data-path ./data/ --output-path ./output/
```

**Flow**:
```
main() → Parse args → Create pipeline → Load data → Run 7 analyses → Save results
```

---

#### `batch_main_v2.py` (324 dòng)
**Chức năng**: Entry point cho phiên bản V2 với tính năng nâng cao

**Class chính**:
- `EnhancedYelpPipeline` - Kế thừa từ `YelpAnalysisPipeline`, bổ sung 2 analyses mới

**Các hàm chính**:
- `run_analysis_8()` - Trending Businesses (Window Functions)
- `run_analysis_9()` - Category Performance Matrix (Pivot/Unpivot)
- `run_all_analyses_v2()` - Chạy tất cả 9 analyses

**Cách sử dụng**:
```bash
python batch_main_v2.py --data-path ./data/ --output-path ./output_v2/
```

**Flow**:
```
main() → Create EnhancedPipeline → Load data → Run 9 analyses → Save results
```

---

### 2️⃣ **CORE MODULES** - Module cốt lõi

#### `batch_configuration.py` (127 dòng)
**Chức năng**: Quản lý cấu hình Spark và định nghĩa schemas

**Class chính**:
- `SparkConfig` - Cấu hình Spark Session
- `DataSchemas` - Định nghĩa schemas cho Business, Review, User

**Các hàm quan trọng**:
```python
SparkConfig.create_spark_session()  # Tạo Spark Session với config tối ưu
DataSchemas.business_schema()       # Schema cho business.json
DataSchemas.review_schema()         # Schema cho review.json
DataSchemas.user_schema()           # Schema cho user.json
```

**Spark Config**:
- Driver memory: 8GB
- Executor memory: 4GB
- Shuffle partitions: 20
- Adaptive Query Execution: enabled
- Serializer: Kryo

---

#### `batch_load_data.py` (122 dòng)
**Chức năng**: Load dữ liệu từ file JSON vào DataFrame

**Class chính**:
- `DataLoader` - Quản lý việc load data

**Các hàm chính**:
```python
load_business_data()  # Load business.json → business_df
load_review_data()    # Load review.json → review_df
load_user_data()      # Load user.json → user_df (optional)
```

**Input**: JSON files
**Output**: Spark DataFrames với schema đã định nghĩa

**Ví dụ**:
```python
loader = DataLoader(spark, "./data/")
business_df = loader.load_business_data()  # Load 150,346 businesses
review_df = loader.load_review_data()      # Load 6,990,280 reviews
```

---

#### `batch_pipeline.py` (229 dòng)
**Chức năng**: Orchestrator chính điều phối toàn bộ pipeline

**Class chính**:
- `YelpAnalysisPipeline` - Pipeline orchestrator

**Các hàm chính**:
```python
load_data()           # Load tất cả datasets
run_analysis_1()      # Top Selling Products
run_analysis_2()      # User Purchase Patterns
run_analysis_3()      # Top Users by Reviews
run_analysis_4()      # Category Trends Over Time
run_analysis_5()      # High Rating Low Review Count
run_analysis_6()      # Geographic Distribution
run_analysis_7()      # Seasonal Trends
run_all_analyses()    # Chạy tất cả 7 analyses
save_results()        # Lưu kết quả ra file
```

**Dependency**:
- Sử dụng `batch_configuration.py` để khởi tạo Spark
- Sử dụng `batch_load_data.py` để load data
- Sử dụng `batch_analytics.py` để chạy analyses

**Flow**:
```
Pipeline.__init__() → load_data() → run_analysis_X() → save_results()
```

---

### 3️⃣ **ANALYTICS MODULES** - Module phân tích

#### `batch_analytics.py` (335 dòng)
**Chức năng**: 7 analyses cơ bản với Broadcast Join optimization

**Class chính**:
- `YelpAnalytics` - Container cho 7 hàm phân tích

**Danh sách 7 Analyses**:

| ID | Tên Analysis | Hàm | Tính năng | Output |
|---|---|---|---|---|
| 1 | Top Selling Products | `top_selling_products_recent()` | Broadcast Join | Top businesses theo review gần đây |
| 2 | User Purchase Patterns | `user_purchase_patterns()` | Aggregation | Phân tích hành vi user |
| 3 | Top Users by Reviews | `top_users_by_reviews()` | Broadcast Join | Top users theo số lượng review |
| 4 | Category Trends | `category_trends_over_time()` | Broadcast Join | Xu hướng theo categories |
| 5 | High Rating Low Review | `high_rating_low_review_businesses()` | Filter | Businesses ít review nhưng rating cao |
| 6 | Geographic Distribution | `geographic_distribution()` | Broadcast Join | Phân bố theo địa lý |
| 7 | Seasonal Trends | `seasonal_trends()` | Time-based | Xu hướng theo mùa |

**Tính năng nổi bật**:
- ✅ **Broadcast Join**: Optimized cho join với bảng nhỏ (analyses 1, 3, 4, 6)
- ✅ **Caching**: Cache DataFrame để tái sử dụng
- ✅ **Salted Aggregation**: Xử lý data skew

**Ví dụ sử dụng**:
```python
analytics = YelpAnalytics()
result = analytics.top_selling_products_recent(
    review_df, business_df, days=15, top_n=10
)
result.show()
```

---

#### `batch_analytics_advanced.py` (473 dòng)
**Chức năng**: 2 analyses nâng cao với Window Functions và Pivot/Unpivot

**Class chính**:
- `AdvancedYelpAnalytics` - Container cho analyses nâng cao

**Danh sách 2 Analyses Nâng cao**:

| ID | Tên Analysis | Hàm | Tính năng | Output |
|---|---|---|---|---|
| 8 | Trending Businesses | `trending_businesses()` | Window Functions | Businesses đang trending với growth rate |
| 9 | Category Performance Matrix | `category_performance_matrix()` | Pivot/Unpivot | Ma trận performance theo category & city |

**Analysis 8 - Trending Businesses**:
```python
trending_businesses(review_df, business_df, window_days=90, top_n=10)
```
**Window Functions sử dụng**:
- `lag()` - So sánh với tuần trước
- `lead()` - Nhìn trước tuần sau
- `avg() over window` - Moving average 4 tuần
- `dense_rank()` - Ranking businesses theo growth rate
- `rowsBetween(-3, 0)` - Sliding window

**Output**: Top businesses có tốc độ tăng trưởng cao nhất

---

**Analysis 9 - Category Performance Matrix**:
```python
category_performance_matrix(business_df, review_df, top_categories=10, top_cities=5)
```
**Pivot/Unpivot Operations**:
- `pivot()` - Chuyển long → wide format (categories làm rows, cities làm columns)
- `stack()` - Chuyển wide → long format (unpivot)
- Cross-tabulation analysis

**Output**: Ma trận hiệu suất categories × cities với avg rating & review count

---

### 4️⃣ **UDF LIBRARY** - Thư viện hàm tùy chỉnh

#### `batch_udf.py` (443 dòng)
**Chức năng**: Thư viện 7 User Defined Functions

**Danh sách UDFs**:

#### **Regular UDFs** (Xử lý từng row - chậm hơn)

| UDF | Input | Output | Chức năng |
|---|---|---|---|
| `categorize_rating()` | stars: int | string | Phân loại rating: "Excellent" / "Good" / "Average" / "Poor" |
| `is_weekend()` | date: datetime | boolean | Kiểm tra có phải cuối tuần không |
| `extract_city_state()` | address: string | string | Trích xuất "City, State" từ địa chỉ |

**Ví dụ**:
```python
from batch_udf import categorize_rating

df = df.withColumn("rating_category", categorize_rating(col("stars")))
# Output: 5 stars → "Excellent", 1 star → "Poor"
```

---

#### **Pandas UDFs** (Xử lý vectorized - nhanh hơn 10-100x)

| UDF | Input | Output | Chức năng |
|---|---|---|---|
| `sentiment_score()` | text: string | float | Điểm sentiment từ 0.0-1.0 (0=negative, 1=positive) |
| `extract_keywords()` | text: string | string | Trích xuất top 3 keywords quan trọng |
| `text_length_normalized()` | text: string | float | Độ dài text chuẩn hóa 0.0-1.0 |
| `extract_hashtags()` | text: string | string | Trích xuất hashtags từ text |

**Ví dụ**:
```python
from batch_udf import sentiment_score

df = df.withColumn("sentiment", sentiment_score(col("text")))
# Output: "Great food!" → 0.9, "Terrible service" → 0.1
```

**Performance Comparison**:
- Regular UDF: Process 10,000 rows ~ 5-10 seconds
- Pandas UDF: Process 10,000 rows ~ 0.1-0.5 seconds
- **Tốc độ cải thiện: 10-100x**

---

### 5️⃣ **TESTING** - Kiểm thử

#### `test_local_features.py` (355 dòng)
**Chức năng**: Test suite toàn diện cho 4 tính năng nâng cao

**Class chính**:
- `FeatureTester` - Test orchestrator

**Các test functions**:

| Test | Hàm | Kiểm tra |
|---|---|---|
| UDF Test | `test_udfs()` | Test 7 UDFs, so sánh performance Regular vs Pandas |
| Window Functions | `test_window_functions()` | Test Analysis 8, verify lag/lead/rank |
| Pivot/Unpivot | `test_pivot_unpivot()` | Test Analysis 9, validate pivot/unpivot |
| Broadcast Join | `test_broadcast_join()` | Check physical plan có BroadcastHashJoin |

**Cách sử dụng**:
```bash
# Test từng feature
python test_local_features.py --test udf
python test_local_features.py --test window
python test_local_features.py --test pivot
python test_local_features.py --test broadcast

# Test tất cả
python test_local_features.py --test all
```

**Output**: In ra kết quả test, performance metrics, và status (✓/✗)

---

### 6️⃣ **UTILITIES** - Tiện ích

#### `create_sample_data.py` (119 dòng)
**Chức năng**: Tạo dữ liệu mẫu để test nếu không có data thật

**Hàm chính**:
```python
create_sample_business_data()  # Tạo 100 businesses mẫu
create_sample_review_data()    # Tạo 1000 reviews mẫu
```

**Output**:
- `data/sample_business.json` - 100 businesses
- `data/sample_review.json` - 1000 reviews

**Cách sử dụng**:
```bash
python create_sample_data.py
```

---

## 🔄 DATA FLOW - Luồng dữ liệu

### Pipeline V1 (Basic)
```
┌─────────────────┐
│  batch_main.py  │ Entry Point
└────────┬────────┘
         │
         ▼
┌──────────────────────┐
│ batch_pipeline.py    │ Orchestrator
│ YelpAnalysisPipeline │
└──────────┬───────────┘
           │
           ├───► batch_configuration.py (Spark Config)
           │
           ├───► batch_load_data.py (Load Data)
           │         │
           │         ├─ business.json → business_df
           │         └─ review.json → review_df
           │
           └───► batch_analytics.py (7 Analyses)
                     │
                     ├─ Analysis 1: Top Selling
                     ├─ Analysis 2: User Patterns
                     ├─ Analysis 3: Top Users
                     ├─ Analysis 4: Category Trends
                     ├─ Analysis 5: High Rating Low Review
                     ├─ Analysis 6: Geographic
                     └─ Analysis 7: Seasonal
                           │
                           ▼
                  ┌────────────────┐
                  │  output/*.csv  │ Results
                  └────────────────┘
```

### Pipeline V2 (Advanced)
```
┌──────────────────┐
│ batch_main_v2.py │ Entry Point
└────────┬─────────┘
         │
         ▼
┌─────────────────────────┐
│ batch_main_v2.py        │
│ EnhancedYelpPipeline    │ (kế thừa YelpAnalysisPipeline)
└──────────┬──────────────┘
           │
           ├───► batch_pipeline.py (Base Pipeline)
           │          │
           │          └─► Chạy 7 analyses cơ bản
           │
           ├───► batch_udf.py (UDF Library)
           │          │
           │          └─► 7 UDFs (3 Regular + 4 Pandas)
           │
           └───► batch_analytics_advanced.py (2 Analyses Nâng cao)
                      │
                      ├─ Analysis 8: Trending (Window Functions)
                      └─ Analysis 9: Performance Matrix (Pivot/Unpivot)
                            │
                            ▼
                   ┌─────────────────┐
                   │ output_v2/*.csv │ Results
                   └─────────────────┘
```

---

## 🎯 FUNCTION MAPPING - Hàm nào ở đâu?

### Cần tạo Spark Session?
➜ **`batch_configuration.py`**
```python
from batch_configuration import SparkConfig
spark = SparkConfig.create_spark_session()
```

### Cần load dữ liệu?
➜ **`batch_load_data.py`**
```python
from batch_load_data import DataLoader
loader = DataLoader(spark, "./data/")
business_df = loader.load_business_data()
```

### Cần chạy analyses cơ bản?
➜ **`batch_analytics.py`**
```python
from batch_analytics import YelpAnalytics
analytics = YelpAnalytics()
result = analytics.top_selling_products_recent(review_df, business_df)
```

### Cần chạy analyses nâng cao?
➜ **`batch_analytics_advanced.py`**
```python
from batch_analytics_advanced import AdvancedYelpAnalytics
advanced = AdvancedYelpAnalytics()
result = advanced.trending_businesses(review_df, business_df)
```

### Cần sử dụng UDF?
➜ **`batch_udf.py`**
```python
from batch_udf import sentiment_score, categorize_rating
df = df.withColumn("sentiment", sentiment_score(col("text")))
```

### Cần chạy toàn bộ pipeline?
➜ **`batch_main.py`** (V1) hoặc **`batch_main_v2.py`** (V2)
```bash
python batch_main_v2.py --data-path ./data/
```

---

## 🔗 DEPENDENCY GRAPH - Quan hệ giữa các modules

```
batch_main_v2.py
    │
    ├─► batch_pipeline.py
    │       │
    │       ├─► batch_configuration.py
    │       ├─► batch_load_data.py
    │       │       └─► batch_configuration.py (schemas)
    │       └─► batch_analytics.py
    │
    ├─► batch_analytics_advanced.py
    │       └─► batch_udf.py
    │
    └─► (không phụ thuộc trực tiếp)
            test_local_features.py
            create_sample_data.py
```

**Dependencies theo module**:

| Module | Depends On |
|---|---|
| `batch_configuration.py` | pyspark (external) |
| `batch_load_data.py` | batch_configuration.py |
| `batch_analytics.py` | pyspark.sql.functions |
| `batch_analytics_advanced.py` | batch_udf.py |
| `batch_pipeline.py` | batch_configuration, batch_load_data, batch_analytics |
| `batch_main.py` | batch_pipeline |
| `batch_main_v2.py` | batch_pipeline, batch_analytics_advanced |
| `test_local_features.py` | ALL analytics modules |

---

## 📝 QUICK REFERENCE - Tra cứu nhanh

### Muốn chạy toàn bộ pipeline?
```bash
# V1: 7 analyses cơ bản
python batch_main.py --data-path ./data/

# V2: 9 analyses (7 cơ bản + 2 nâng cao)
python batch_main_v2.py --data-path ./data/
```

### Muốn test các tính năng nâng cao?
```bash
# Test tất cả
python test_local_features.py --test all

# Test từng feature
python test_local_features.py --test udf
python test_local_features.py --test window
python test_local_features.py --test pivot
python test_local_features.py --test broadcast
```

### Muốn tạo dữ liệu mẫu?
```bash
python create_sample_data.py
```

### Muốn sử dụng từng module riêng lẻ?
```python
# Example: Chỉ chạy Analysis 8
from batch_configuration import SparkConfig
from batch_load_data import DataLoader
from batch_analytics_advanced import AdvancedYelpAnalytics

spark = SparkConfig.create_spark_session()
loader = DataLoader(spark, "./data/")
review_df = loader.load_review_data()
business_df = loader.load_business_data()

advanced = AdvancedYelpAnalytics()
result = advanced.trending_businesses(review_df, business_df, window_days=90, top_n=10)
result.show()
```

---

## 🎓 HỌC TỪ CODE NÀY

### Patterns sử dụng trong dự án:

1. **Separation of Concerns**: Mỗi module có trách nhiệm rõ ràng
   - Configuration → `batch_configuration.py`
   - Data Loading → `batch_load_data.py`
   - Analytics Logic → `batch_analytics.py`, `batch_analytics_advanced.py`
   - Orchestration → `batch_pipeline.py`

2. **Class-based Architecture**: Sử dụng static methods cho analytics
   ```python
   class YelpAnalytics:
       @staticmethod
       def analysis_name():
           # Logic
   ```

3. **Inheritance**: EnhancedPipeline kế thừa YelpAnalysisPipeline
   ```python
   class EnhancedYelpPipeline(YelpAnalysisPipeline):
       # Extends functionality
   ```

4. **Performance Optimization**:
   - Broadcast Join cho small tables
   - Caching DataFrame để reuse
   - Pandas UDF cho vectorization
   - Salted Aggregation cho data skew

5. **Error Handling**: Try-except trong mỗi analysis function

6. **Modularity**: Mỗi analysis có thể chạy độc lập

---

## 📊 STATISTICS - Thống kê dự án

| Metric | Value |
|---|---|
| **Tổng số dòng code** | 2,712 dòng Python |
| **Số modules** | 9 Python files |
| **Số analyses** | 9 (7 basic + 2 advanced) |
| **Số UDFs** | 7 (3 Regular + 4 Pandas) |
| **Spark features** | 15+ (Join, Window, Pivot, UDF, Broadcast, etc.) |
| **Test coverage** | 4 feature tests |
| **Documentation** | 5 markdown files |

---

## 🚀 NEXT STEPS - Bước tiếp theo

### Để bắt đầu:
1. ✅ Đọc file này để hiểu cấu trúc
2. ✅ Đọc `LOCAL_TEST_GUIDE.md` để biết cách test
3. ✅ Chạy `python test_local_features.py --test all`
4. ✅ Chạy `python batch_main_v2.py --data-path ./data/`

### Để mở rộng:
- Thêm analyses mới vào `batch_analytics_advanced.py`
- Thêm UDFs mới vào `batch_udf.py`
- Thêm test cases vào `test_local_features.py`
- Tối ưu performance trong `batch_configuration.py`

---

## ❓ CÂU HỎI THƯỜNG GẶP

**Q: Tại sao có 2 entry points (batch_main.py và batch_main_v2.py)?**
A: `batch_main.py` là phiên bản V1 cơ bản với 7 analyses. `batch_main_v2.py` là V2 nâng cao với 9 analyses + tính năng Spark nâng cao.

**Q: Tôi nên sử dụng file nào để chạy?**
A: Sử dụng `batch_main_v2.py` để có đầy đủ tính năng nhất (9 analyses).

**Q: Làm sao biết code có hoạt động không?**
A: Chạy `python test_local_features.py --test all` để kiểm tra.

**Q: Muốn thêm analysis mới thì sửa file nào?**
A: Thêm hàm static method vào `batch_analytics_advanced.py`, sau đó thêm `run_analysis_X()` vào `EnhancedYelpPipeline` trong `batch_main_v2.py`.

**Q: UDF nào nhanh hơn, Regular hay Pandas?**
A: Pandas UDF nhanh hơn 10-100x vì xử lý vectorized. Sử dụng Pandas UDF khi có thể.

---

## 📞 LIÊN HỆ & HỖ TRỢ

- **Documentation**: Xem `LOCAL_TEST_GUIDE.md` cho hướng dẫn chi tiết
- **Quick Start**: Xem `QUICKSTART.md`
- **Implementation Plan**: Xem `IMPLEMENTATION_PLAN_PA1.md`

---

**Last Updated**: 2025-12-16
**Version**: 2.0 (Advanced Features)
**Author**: Claude Code Pipeline
