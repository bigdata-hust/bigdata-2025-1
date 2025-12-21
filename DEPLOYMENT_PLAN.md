# 📋 PHƯƠNG ÁN TRIỂN KHAI NHÁNH HAI - LOCAL EXECUTION

## 🔍 PHÂN TÍCH KIẾN TRÚC HIỆN TẠI

### Luồng dữ liệu của nhánh HAI:
```
Data Files (JSON)
    ↓
Kafka Producer → Kafka Topics (business, review, user)
    ↓
PySpark Streaming Consumer → 7 Analytics Functions
    ↓
Output: HDFS + Elasticsearch + MongoDB
    ↓
Visualization: Kibana + Web Dashboard
```

### Điểm mạnh:
✅ Real-time streaming architecture
✅ Production-ready với fault tolerance
✅ Scalable với HDFS distributed storage
✅ Full observability với Kibana

### Thách thức khi chạy local:
❌ Yêu cầu 7+ Docker containers (Kafka, Zookeeper, HDFS namenode/datanode, Elasticsearch, Kibana, Producer, Spark)
❌ Tốn ~8-16GB RAM
❌ Setup phức tạp (~30 phút)
❌ Code hiện tại dùng `readStream` → không thể chạy trực tiếp với file local

---

## 🎯 2 PHƯƠNG ÁN TRIỂN KHAI

---

## ⭐ PHƯƠNG ÁN 1: SIMPLIFIED BATCH MODE (KHUYẾN NGHỊ)

### Mô tả:
Chuyển đổi code từ **Streaming** sang **Batch processing** để chạy nhanh trên local với CSV/JSON files.

### Kiến trúc đơn giản hóa:
```
Local Data Files (CSV/JSON)
    ↓
PySpark Batch Processing → 7 Analytics
    ↓
Output Console + Elasticsearch (optional)
    ↓
Kibana Visualization (optional)
```

### Yêu cầu tài nguyên:
- **RAM**: 4-8GB
- **Services**:
  - Bắt buộc: Python + PySpark
  - Tùy chọn: Elasticsearch + Kibana (chỉ khi cần visualize)

### Thay đổi code cần thiết:

#### 1. Sửa file `load_data.py`:
```python
# BEFORE (Streaming):
business_df = spark.readStream.format("kafka")...

# AFTER (Batch):
business_df = spark.read.json("data/business.json", schema=...)
```

#### 2. Sửa file `pipeline_orchestration.py`:
```python
# BEFORE: writeStream
df.writeStream.format("parquet")...

# AFTER: write
df.write.mode("overwrite").parquet("output/...")
```

#### 3. Bỏ watermark và streaming configs:
```python
# REMOVE:
.withWatermark('business_ts', '10 minutes')
.awaitAnyTermination()
```

### Các bước thực hiện:

#### Bước 1: Chuẩn bị môi trường
```bash
# Clone hoặc đã có repository
cd bigdata-2025-1

# Tạo virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# hoặc: venv\Scripts\activate  # Windows

# Cài đặt dependencies
pip install pyspark==4.0.1 requests
```

#### Bước 2: Chuẩn bị dữ liệu
```bash
# Tạo thư mục data
mkdir -p data

# Copy hoặc download data files vào thư mục:
# - data/business.json
# - data/review.json
# - data/user.json (optional)
```

#### Bước 3: Tạo phiên bản Batch của code
```bash
# Tạo thư mục mới cho batch version
mkdir -p Spark/batch_mode

# Copy các file và modify:
# - batch_load_data.py (version batch của load_data.py)
# - batch_pipeline.py (version batch của pipeline_orchestration.py)
# - batch_main.py (version batch của main.py)
# - analytics_yelp.py (giữ nguyên)
# - configuration.py (bỏ streaming configs)
```

#### Bước 4: Chạy pipeline
```bash
cd Spark/batch_mode
python batch_main.py
```

#### Bước 5: Xem kết quả
```bash
# Kết quả sẽ hiển thị trên console
# Hoặc được lưu vào output/
ls -la output/
```

### Ưu điểm:
✅ Đơn giản, chỉ cần Python + PySpark
✅ Chạy nhanh (~5-10 phút với small dataset)
✅ Không cần Docker
✅ Dễ debug
✅ Phù hợp cho development & testing

### Nhược điểm:
❌ Mất tính năng real-time
❌ Không giống production
❌ Cần modify code (nhưng không lớn)

---

## 🚀 PHƯƠNG ÁN 2: FULL STACK DOCKER (PRODUCTION-LIKE)

### Mô tả:
Chạy đúng như production với tất cả services trong Docker.

### Kiến trúc:
```
Docker Compose Stack:
├── Zookeeper
├── Kafka
├── Kafka Producer (streaming data)
├── HDFS Namenode
├── HDFS Datanode
├── Elasticsearch
├── Kibana
└── Spark Container (processing)
```

### Yêu cầu tài nguyên:
- **RAM**: 12-16GB
- **Disk**: ~5GB cho Docker images
- **CPU**: 4+ cores khuyến nghị

### Yêu cầu cần có:
✅ Docker & Docker Compose đã cài
✅ Data files (business.json, review.json, user.json)
✅ Đủ RAM

### Các bước thực hiện:

#### Bước 1: Chuẩn bị dữ liệu
```bash
cd bigdata-2025-1

# Tạo thư mục data nếu chưa có
mkdir -p data

# Copy data files vào:
# - data/business.json
# - data/review.json
# - data/user.json
```

#### Bước 2: Kiểm tra docker-compose.yml
```bash
# File đã có sẵn trong nhánh HAI
cat docker-compose.yml

# Đảm bảo volume mapping đúng:
# - ./data:/app/data  (cho kafka-producer)
```

#### Bước 3: Khởi động stack
```bash
# Start tất cả services
docker-compose up -d

# Đợi ~2-3 phút cho services khởi động
# Kiểm tra status
docker-compose ps

# Expected output: 8 services running
```

#### Bước 4: Verify services
```bash
# Kiểm tra Kafka
docker exec -it $(docker-compose ps -q kafka) \
  kafka-topics.sh --list --bootstrap-server localhost:9092

# Kiểm tra Elasticsearch
curl http://localhost:9200

# Kiểm tra Kibana
curl http://localhost:5601

# Kiểm tra HDFS
curl http://localhost:9870
```

#### Bước 5: Monitor Kafka Producer
```bash
# Xem logs của producer (data được stream vào Kafka)
docker-compose logs -f kafka-producer

# Sẽ thấy: "📦 Sent 100 records to topic 'business'"
```

#### Bước 6: Chạy Spark pipeline
```bash
# Pipeline tự động chạy trong spark container
docker-compose logs -f spark

# Hoặc trigger manually:
docker exec -it $(docker-compose ps -q spark) \
  spark-submit /app/Spark/main.py
```

#### Bước 7: Xem kết quả trong Elasticsearch
```bash
# Liệt kê các indices
curl http://localhost:9200/_cat/indices?v

# Expected: top_selling, diverse_stores, best_rated, most_positive,
#           peak_hours, top_categories, store_stats

# Xem data trong một index
curl http://localhost:9200/top_selling/_search?size=10&pretty
```

#### Bước 8: Visualize trong Kibana
```bash
# Mở browser
open http://localhost:5601

# Tạo Index Patterns:
# - Go to: Stack Management → Index Patterns
# - Create pattern: top_selling*
# - Repeat cho 6 indices khác

# Tạo Visualizations & Dashboard
```

#### Bước 9: Kiểm tra HDFS
```bash
# List files in HDFS
docker exec -it hdfs-namenode \
  hdfs dfs -ls /test_01/

# Expected: 7 directories (một cho mỗi analysis)
```

### Ưu điểm:
✅ Giống production 100%
✅ Real-time streaming
✅ Full observability
✅ Scalable
✅ Không cần modify code

### Nhược điểm:
❌ Tốn tài nguyên lớn
❌ Setup phức tạp
❌ Debug khó hơn
❌ Chạy lâu (~30 phút setup + run)

---

## 📊 SO SÁNH 2 PHƯƠNG ÁN

| Tiêu chí | Phương án 1 (Batch) | Phương án 2 (Docker) |
|----------|---------------------|----------------------|
| **RAM cần** | 4-8GB | 12-16GB |
| **Setup time** | 5 phút | 30 phút |
| **Độ phức tạp** | Thấp ⭐ | Cao ⭐⭐⭐ |
| **Modify code** | Có (nhỏ) | Không |
| **Real-time** | ❌ | ✅ |
| **Giống production** | 40% | 100% |
| **Debug** | Dễ | Khó |
| **Phù hợp cho** | Dev, Test, Demo | Production, Full test |

---

## 🎯 KHUYẾN NGHỊ

### Chọn Phương án 1 (Batch) nếu:
- ✅ Chỉ muốn xem kết quả 7 hàm phân tích nhanh
- ✅ Máy có RAM hạn chế (< 12GB)
- ✅ Đang trong giai đoạn development/testing
- ✅ Muốn demo nhanh
- ✅ Không cần real-time

### Chọn Phương án 2 (Docker) nếu:
- ✅ Muốn test full production stack
- ✅ Máy đủ mạnh (12GB+ RAM)
- ✅ Cần test real-time streaming
- ✅ Muốn hiểu rõ toàn bộ kiến trúc
- ✅ Có thời gian setup

---

## 📝 CẤU TRÚC 7 HÀM PHÂN TÍCH

Cả 2 phương án đều chạy được 7 hàm phân tích sau:

### 1. Top Selling Products (Recent)
- **Input**: review_df, business_df
- **Logic**: Đếm reviews trong N ngày gần đây
- **Output**: Top 10 businesses có nhiều review nhất

### 2. Diverse Stores
- **Input**: business_df
- **Logic**: Đếm số lượng categories mỗi store
- **Output**: Top 10 stores có nhiều categories nhất

### 3. Best Rated Products
- **Input**: business_df, review_df
- **Logic**: Tính avg stars, filter min_reviews
- **Output**: Top 10 businesses rating cao nhất

### 4. Most Positive Reviews
- **Input**: business_df, review_df
- **Logic**: Đếm reviews >= 4 sao, tính tỷ lệ
- **Output**: Top 10 stores có nhiều positive reviews nhất

### 5. Peak Hours
- **Input**: review_df
- **Logic**: Group by year, month, hour
- **Output**: Thống kê số lượng reviews theo thời gian

### 6. Top Categories
- **Input**: business_df, review_df
- **Logic**: Explode categories, count reviews
- **Output**: Top 20 categories có nhiều reviews nhất

### 7. Store Statistics
- **Input**: business_df, review_df
- **Logic**: Aggregate all businesses với actual stats
- **Output**: Full statistics của tất cả stores

---

## 🛠️ BƯỚC TIẾP THEO

### Nếu chọn Phương án 1:
1. Tôi sẽ tạo các file batch version:
   - `Spark/batch_mode/batch_load_data.py`
   - `Spark/batch_mode/batch_pipeline.py`
   - `Spark/batch_mode/batch_main.py`
   - `Spark/batch_mode/batch_configuration.py`

2. Update code để đọc từ local files

3. Tạo script chạy nhanh: `run_local.sh`

4. Tạo sample data (nếu chưa có)

### Nếu chọn Phương án 2:
1. Verify docker-compose.yml

2. Check data files có sẵn chưa

3. Tạo script helper: `setup_docker.sh`

4. Tạo monitoring dashboard

---

## 🔧 TROUBLESHOOTING

### Phương án 1:
```bash
# Lỗi: Module not found
pip install pyspark==4.0.1 requests

# Lỗi: Java not found
# Install Java 11+: https://adoptium.net/

# Lỗi: Data file not found
# Đảm bảo files ở đúng thư mục data/
```

### Phương án 2:
```bash
# Lỗi: Docker không khởi động
docker-compose down -v
docker-compose up -d

# Lỗi: Out of memory
# Tăng Docker memory limit: Docker Desktop → Settings → Resources

# Lỗi: Port conflict
# Đổi port trong docker-compose.yml

# Lỗi: Kafka connection refused
# Đợi thêm 2-3 phút cho Kafka khởi động hoàn toàn
```

---

## 📞 HỎI ĐÁP

**Q: Tôi nên chọn phương án nào?**
A: Nếu mục đích chỉ là xem kết quả 7 phân tích → Chọn Phương án 1. Nếu muốn test full stack → Chọn Phương án 2.

**Q: Data ở đâu?**
A: Cần có 3 files JSON từ Yelp dataset. Nếu chưa có, tôi có thể tạo sample data nhỏ để test.

**Q: Có thể kết hợp 2 phương án không?**
A: Có! Chạy Phương án 1 trước để verify logic, sau đó chạy Phương án 2 cho production test.

**Q: Thời gian chạy bao lâu?**
A:
- Phương án 1: 5-10 phút (tùy kích thước data)
- Phương án 2: 30-45 phút (setup + run)

**Q: Kết quả hiển thị ở đâu?**
A:
- Phương án 1: Console output + files trong `output/`
- Phương án 2: Elasticsearch + Kibana dashboard

---

## ✅ CHECKLIST TRƯỚC KHI BẮT ĐẦU

### Phương án 1:
- [ ] Python 3.8+ đã cài
- [ ] Java 11+ đã cài
- [ ] Có data files hoặc ready để tạo sample
- [ ] ~5GB disk space trống

### Phương án 2:
- [ ] Docker & Docker Compose đã cài
- [ ] Docker có ít nhất 12GB RAM allocation
- [ ] Có data files (business.json, review.json, user.json)
- [ ] Ports available: 2181, 9092, 9000, 9870, 9200, 5601
- [ ] ~10GB disk space trống

---

**Bạn muốn triển khai phương án nào? Tôi sẽ giúp bạn setup chi tiết từng bước! 🚀**
