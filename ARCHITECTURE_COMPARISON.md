# 🏗️ SO SÁNH KIẾN TRÚC: NHÁNH HAI vs CÁC PHƯƠNG ÁN LOCAL

## 📊 TỔNG QUAN 3 KIẾN TRÚC

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     KIẾN TRÚC GỐC - NHÁNH HAI                           │
│                        (Production Streaming)                            │
└─────────────────────────────────────────────────────────────────────────┘

    Data Files (business.json, review.json, user.json)
         │
         ↓
    ┌────────────────┐
    │ Kafka Producer │  (Stream data vào Kafka)
    │  (Container)   │
    └────────┬───────┘
             │
             ↓
    ┌────────────────────────────────────────┐
    │      KAFKA CLUSTER                     │
    │  ┌───────────┐    ┌──────────────┐   │
    │  │ Zookeeper │ ←→ │ Kafka Broker │   │
    │  └───────────┘    └──────────────┘   │
    │   Topics: business, review, user      │
    └────────────┬───────────────────────────┘
                 │
                 ↓
    ┌─────────────────────────────┐
    │   SPARK STREAMING           │
    │  - readStream from Kafka    │
    │  - 7 Analytics Functions    │
    │  - Watermarking             │
    │  - Checkpointing            │
    └────────┬────────────────────┘
             │
             ├──────────────┬───────────────┬──────────────┐
             ↓              ↓               ↓              ↓
    ┌────────────┐  ┌──────────────┐  ┌──────────┐  ┌──────────┐
    │    HDFS    │  │Elasticsearch │  │ MongoDB  │  │  Console │
    │  (Parquet) │  │  (7 indices) │  │(Optional)│  │  Output  │
    └────────────┘  └──────┬───────┘  └──────────┘  └──────────┘
                           │
                           ↓
                    ┌──────────────┐
                    │    Kibana    │
                    │  Dashboard   │
                    └──────────────┘

    Services: 8+ containers
    RAM: 12-16GB
    Complexity: ⭐⭐⭐⭐⭐
    Setup: 30 phút



┌─────────────────────────────────────────────────────────────────────────┐
│                   PHƯƠNG ÁN 1 - SIMPLIFIED BATCH                        │
│                     (Development/Testing Mode)                           │
└─────────────────────────────────────────────────────────────────────────┘

    Local Data Files (CSV/JSON/Parquet)
         │
         ↓
    ┌────────────────────────────┐
    │   SPARK BATCH (Local)      │
    │  - spark.read() from files │
    │  - 7 Analytics Functions   │
    │  - No streaming overhead   │
    │  - Direct processing       │
    └────────┬───────────────────┘
             │
             ├──────────────┬─────────────────┐
             ↓              ↓                 ↓
    ┌────────────┐  ┌──────────────┐  ┌──────────────┐
    │   Local    │  │Elasticsearch │  │   Console    │
    │   Files    │  │  (Optional)  │  │   Output     │
    │ (Parquet)  │  └──────┬───────┘  │  (show())    │
    └────────────┘         │          └──────────────┘
                           ↓
                    ┌──────────────┐
                    │    Kibana    │
                    │  (Optional)  │
                    └──────────────┘

    Services: 0-2 containers (chỉ ES+Kibana nếu cần)
    RAM: 4-8GB
    Complexity: ⭐⭐
    Setup: 5 phút



┌─────────────────────────────────────────────────────────────────────────┐
│                PHƯƠNG ÁN 2 - FULL DOCKER STACK                          │
│                    (Production Simulation)                               │
└─────────────────────────────────────────────────────────────────────────┘

    [GIỐNG KIẾN TRÚC GỐC - Chạy trong Docker Compose]

    docker-compose up -d

    → 8 containers khởi động tự động
    → Tất cả services kết nối qua Docker network
    → Data flow tự động từ Producer → Kafka → Spark → Outputs

    Services: 8 containers
    RAM: 12-16GB
    Complexity: ⭐⭐⭐⭐
    Setup: 30 phút
```

---

## 🔄 LUỒNG XỬ LÝ 7 HÀM PHÂN TÍCH

### Nhánh HAI (Streaming):
```python
# 1. Load data từ Kafka
business_df = spark.readStream.format("kafka")...
review_df = spark.readStream.format("kafka")...

# 2. Run analytics (streaming mode)
result = YelpAnalytics.top_selling_products_recent(review_df, business_df)
# → result là streaming DataFrame

# 3. Output (streaming write)
result.writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", "hdfs://...")
    .start()
    .awaitTermination()  # Chờ streaming chạy liên tục
```

### Phương án 1 (Batch):
```python
# 1. Load data từ file local
business_df = spark.read.json("data/business.json")
review_df = spark.read.json("data/review.json")

# 2. Run analytics (batch mode)
result = YelpAnalytics.top_selling_products_recent(review_df, business_df)
# → result là static DataFrame

# 3. Output (batch write)
result.show(10, truncate=False)  # Hiển thị console
result.write.mode("overwrite").parquet("output/top_selling/")
```

---

## 📋 CHI TIẾT 7 HÀM PHÂN TÍCH

| # | Tên Analysis | Input | Logic chính | Output Columns |
|---|--------------|-------|-------------|----------------|
| 1 | **Top Selling Products** | review_df + business_df | Filter N days gần đây → Count reviews by business → Join with business info | business_id, name, city, recent_review_count, avg_rating |
| 2 | **Diverse Stores** | business_df | Split categories → Count unique categories per store | business_id, name, city, categories, category_count |
| 3 | **Best Rated** | business_df + review_df | Calc avg stars per business → Filter min reviews → Sort by rating | business_id, name, total_reviews, avg_review_stars, business_avg_stars |
| 4 | **Positive Reviews** | business_df + review_df | Count reviews >= threshold stars → Calc positive ratio | business_id, name, positive_review_count, positive_ratio |
| 5 | **Peak Hours** | review_df | Extract year, month, hour from timestamp → Count by time | year, month, hour, review_count |
| 6 | **Top Categories** | business_df + review_df | Explode categories → Join reviews → Count by category | category, total_reviews |
| 7 | **Store Stats** | business_df + review_df | Aggregate all metrics per store | business_id, name, actual_review_count, actual_avg_stars |

---

## 🎯 MAPPING DEPENDENCIES

### Để chạy cả 7 hàm, bạn CẦN:

#### Kiến trúc GỐC (Docker Full):
```yaml
services:
  zookeeper:      # Port 2181
  kafka:          # Port 9092
  kafka-producer: # Push data vào Kafka
  hdfs-namenode:  # Port 9000, 9870
  hdfs-datanode:  # Data storage
  elasticsearch:  # Port 9200
  kibana:         # Port 5601
  spark:          # Processing engine
```

#### Phương án 1 (Batch Simplified):
```bash
# Bắt buộc:
- Python 3.8+
- Java 11+
- PySpark 4.0.1
- Data files (business.json, review.json)

# Tùy chọn (nếu muốn visualize):
- Elasticsearch container
- Kibana container
```

#### Phương án 2 (Docker):
```bash
# Same as Kiến trúc GỐC
- Docker + Docker Compose
- 12GB+ RAM allocation
- Data files mount vào container
```

---

## 💾 YÊU CẦU DATA FILES

### Format data cần có:

#### business.json (hoặc CSV):
```json
{
  "business_id": "abc123",
  "name": "Example Restaurant",
  "city": "Phoenix",
  "state": "AZ",
  "categories": "Food, Restaurant, Italian",
  "stars": 4.5,
  "review_count": 120,
  "is_open": 1,
  "latitude": 33.4484,
  "longitude": -112.0740
}
```

#### review.json:
```json
{
  "review_id": "xyz789",
  "business_id": "abc123",
  "user_id": "user456",
  "stars": 5.0,
  "date": "2022-01-15 10:30:00",
  "text": "Great food!",
  "useful": 10,
  "funny": 2,
  "cool": 5
}
```

### Kích thước data:
- **Small** (Test): 1K businesses, 10K reviews → ~50MB
- **Medium** (Dev): 10K businesses, 100K reviews → ~500MB
- **Large** (Production): 100K+ businesses, 1M+ reviews → 5GB+

---

## ⚙️ CODE MODIFICATIONS CHO PHƯƠNG ÁN 1

### File: `batch_load_data.py`
```python
# BEFORE (Streaming):
def load_business_data(self):
    return (self.spark.readStream
        .format("kafka")
        .option("subscribe", "business")
        .load()
        .select(from_json(...))
    )

# AFTER (Batch):
def load_business_data(self):
    return self.spark.read.json(
        f"{self.data_path}/business.json",
        schema=self.schemas.business_schema()
    )
```

### File: `batch_pipeline.py`
```python
# BEFORE (Streaming):
def save_hdfs(self):
    df.writeStream
        .format("parquet")
        .option("checkpointLocation", "...")
        .start()
        .awaitTermination()

# AFTER (Batch):
def save_local(self):
    df.write
        .mode("overwrite")
        .parquet(f"{self.output_path}/{name}/")
```

### File: `batch_configuration.py`
```python
# REMOVE streaming configs:
- .config("spark.streaming.stopGracefullyOnShutdown", "true")
- .config("spark.sql.streaming.stateStore.providerClass", ...)
- spark.sparkContext.setCheckpointDir(...)

# KEEP batch configs:
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "4g")
```

### File: `analytics_yelp.py`
```python
# NO CHANGE NEEDED!
# Các hàm analytics đã được viết để work với cả streaming và batch
# Chỉ cần remove các dòng .withWatermark() nếu có
```

---

## 🚦 DECISION TREE: CHỌN PHƯƠNG ÁN NÀO?

```
Bắt đầu
   │
   ├─→ Mục đích gì?
   │      │
   │      ├─→ Chỉ xem kết quả 7 phân tích?
   │      │      └─→ PHƯƠNG ÁN 1 ✅
   │      │
   │      ├─→ Test full production stack?
   │      │      └─→ PHƯƠNG ÁN 2 ✅
   │      │
   │      └─→ Cả hai?
   │             └─→ PA1 trước, PA2 sau ✅
   │
   ├─→ RAM có bao nhiêu?
   │      │
   │      ├─→ < 8GB
   │      │      └─→ PHƯƠNG ÁN 1 (chỉ lựa chọn)
   │      │
   │      ├─→ 8-12GB
   │      │      └─→ PHƯƠNG ÁN 1 ✅ (PA2 có thể nhưng chậm)
   │      │
   │      └─→ > 12GB
   │             └─→ Cả PA1 và PA2 đều OK ✅
   │
   ├─→ Có data files chưa?
   │      │
   │      ├─→ Chưa có
   │      │      └─→ PHƯƠNG ÁN 1 (tạo sample nhanh)
   │      │
   │      └─→ Có rồi (JSON format)
   │             └─→ Cả PA1 và PA2 đều OK ✅
   │
   ├─→ Cần real-time không?
   │      │
   │      ├─→ Không, chỉ cần kết quả
   │      │      └─→ PHƯƠNG ÁN 1 ✅
   │      │
   │      └─→ Có, cần test streaming
   │             └─→ PHƯƠNG ÁN 2 ✅
   │
   └─→ Kinh nghiệm Docker?
          │
          ├─→ Ít hoặc không có
          │      └─→ PHƯƠNG ÁN 1 ✅
          │
          └─→ Có kinh nghiệm
                 └─→ PHƯƠNG ÁN 2 OK ✅
```

---

## 📈 PERFORMANCE COMPARISON

| Metric | Nhánh HAI (Docker) | PA1 (Batch) | PA2 (Docker) |
|--------|-------------------|-------------|--------------|
| **Setup Time** | 30 phút | 5 phút | 30 phút |
| **First Run** | 15-20 phút | 5-10 phút | 15-20 phút |
| **Memory Usage** | 12-16GB | 4-8GB | 12-16GB |
| **CPU Usage** | High | Medium | High |
| **Disk I/O** | High | Medium | High |
| **Network** | High (Kafka) | None | High (Kafka) |
| **Startup** | Slow (nhiều services) | Fast | Slow |
| **Debug** | Khó (multi-container) | Dễ (single process) | Khó |
| **Scalability** | Excellent | Good | Excellent |

---

## ✅ FINAL RECOMMENDATION

### 🥇 Nếu là lần đầu & muốn XEM KẾT QUẢ NHANH:
→ **PHƯƠNG ÁN 1 (Batch)**
- Chạy ngay sau 5 phút setup
- Thấy kết quả 7 phân tích trên console
- Không cần Docker
- Dễ debug nếu có lỗi

### 🥈 Nếu muốn HỌC & HIỂU FULL STACK:
→ **PHƯƠNG ÁN 2 (Docker)**
- Giống production
- Hiểu được cách các services kết nối
- Thấy được data flow từ Kafka → Spark → ES
- Tạo dashboard đẹp trên Kibana

### 🥉 Nếu có THỜI GIAN & TÀI NGUYÊN:
→ **CẢ HAI**
1. Chạy PA1 trước để verify logic (30 phút)
2. Chạy PA2 sau để test full stack (1-2 giờ)

---

## 📞 NEXT STEPS

**Sau khi đọc file này, hãy cho tôi biết:**

1. Bạn chọn phương án nào? (1, 2, hay cả hai)
2. Bạn đã có data files chưa?
3. RAM máy bạn bao nhiêu?
4. Có cài Docker chưa?

**Tôi sẽ giúp bạn:**
- Tạo code cho phương án đã chọn
- Setup environment
- Chạy và verify kết quả
- Troubleshoot nếu có lỗi

**Let's get started! 🚀**
