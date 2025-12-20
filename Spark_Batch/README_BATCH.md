# 🚀 Yelp Big Data Analysis - BATCH MODE

**Phiên bản đơn giản hóa để chạy local trên máy tính cá nhân**

---

## 📋 MỤC LỤC

- [Tổng quan](#-tổng-quan)
- [Yêu cầu hệ thống](#-yêu-cầu-hệ-thống)
- [Cài đặt](#-cài-đặt)
- [Chuẩn bị dữ liệu](#-chuẩn-bị-dữ-liệu)
- [Chạy phân tích](#-chạy-phân-tích)
- [Kết quả](#-kết-quả)
- [Tùy chỉnh](#-tùy-chỉnh)
- [Troubleshooting](#-troubleshooting)

---

## 📖 TỔNG QUAN

Phiên bản batch mode này cho phép bạn:
- ✅ Chạy 7 hàm phân tích Yelp trên máy local
- ✅ Không cần Docker, Kafka, HDFS
- ✅ Đọc dữ liệu từ file JSON local
- ✅ Xem kết quả trực tiếp trên console
- ✅ Lưu kết quả ra file (Parquet/CSV/JSON)

### Kiến trúc đơn giản:

```
Local Data Files (JSON)
    ↓
PySpark Batch Processing
    ↓
7 Analytics Functions
    ↓
Console Output + Saved Files
```

### 7 hàm phân tích:

1. **Top Selling Products** - Sản phẩm bán chạy gần đây
2. **Diverse Stores** - Cửa hàng đa dạng nhất
3. **Best Rated** - Đánh giá tốt nhất
4. **Positive Reviews** - Review tích cực nhất
5. **Peak Hours** - Thời gian cao điểm
6. **Top Categories** - Categories phổ biến
7. **Store Statistics** - Thống kê tổng hợp

---

## 💻 YÊU CẦU HỆ THỐNG

### Phần cứng:
- **RAM**: 4GB minimum, 8GB+ khuyến nghị
- **Disk**: ~2GB trống (cho PySpark + output)
- **CPU**: 2+ cores

### Phần mềm:
- **OS**: Ubuntu/Linux (đã test), MacOS, Windows
- **Python**: 3.8 hoặc cao hơn
- **Java**: Java 11 hoặc cao hơn (required cho PySpark)

### Python packages:
- `pyspark==4.0.1`
- `requests`

---

## 🔧 CÀI ĐẶT

### Bước 1: Kiểm tra Python

```bash
python3 --version
# Cần: Python 3.8+
```

Nếu chưa có Python 3.8+:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3 python3-pip

# CentOS/RHEL
sudo yum install python3 python3-pip
```

### Bước 2: Cài đặt Java

```bash
java -version
# Cần: Java 11+
```

Nếu chưa có Java:
```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# Download từ: https://adoptium.net/
```

### Bước 3: Cài đặt PySpark

```bash
# Cài đặt dependencies
pip install pyspark==4.0.1 requests

# Hoặc dùng pip3
pip3 install pyspark==4.0.1 requests
```

### Bước 4: Verify cài đặt

```bash
python3 -c "import pyspark; print(pyspark.__version__)"
# Expected output: 4.0.1
```

---

## 📁 CHUẨN BỊ DỮ LIỆU

### Cấu trúc thư mục cần có:

```
bigdata-2025-1/
├── Spark_Batch/          ← Code ở đây (đã có)
│   ├── batch_main.py
│   ├── batch_configuration.py
│   ├── batch_load_data.py
│   ├── batch_analytics.py
│   ├── batch_pipeline.py
│   ├── run_local.sh
│   └── README_BATCH.md   ← File này
│
└── data/                 ← Data đặt ở đây (cần tạo)
    ├── business.json
    └── review.json
```

### Tạo thư mục data:

```bash
# Di chuyển vào thư mục project
cd /home/user/bigdata-2025-1

# Tạo thư mục data
mkdir -p data
```

### Copy data files:

```bash
# Copy data files của bạn vào thư mục data/
cp /path/to/your/business.json data/
cp /path/to/your/review.json data/

# Verify
ls -lh data/
```

### Format data cần có:

**business.json** (mỗi dòng là 1 JSON object):
```json
{"business_id":"abc123","name":"Restaurant Name","city":"Phoenix","state":"AZ","categories":"Food, Restaurant","stars":4.5,"review_count":120,"is_open":1,"latitude":33.4484,"longitude":-112.074}
```

**review.json** (mỗi dòng là 1 JSON object):
```json
{"review_id":"xyz789","business_id":"abc123","user_id":"user456","stars":5.0,"date":"2022-01-15 10:30:00","text":"Great!","useful":10,"funny":2,"cool":5}
```

---

## 🚀 CHẠY PHÂN TÍCH

### Cách 1: Sử dụng script nhanh (Khuyến nghị)

```bash
# Di chuyển vào thư mục Spark_Batch
cd Spark_Batch

# Chạy với cấu hình mặc định
./run_local.sh

# Chạy với custom data path
./run_local.sh --data-path ../data/

# Chạy và lưu kết quả dạng CSV
./run_local.sh --save-format csv

# Chỉ xem kết quả, không lưu file
./run_local.sh --skip-save

# Xem thêm options
./run_local.sh --help
```

### Cách 2: Chạy trực tiếp với Python

```bash
cd Spark_Batch

# Chạy với cấu hình mặc định
python3 batch_main.py

# Chạy với custom paths
python3 batch_main.py --data-path ../data/ --output-path ../output/

# Xem thêm options
python3 batch_main.py --help
```

### Thời gian chạy dự kiến:

- **Small dataset** (1K businesses, 10K reviews): ~2-5 phút
- **Medium dataset** (10K businesses, 100K reviews): ~5-10 phút
- **Large dataset** (100K+ businesses, 1M+ reviews): ~15-30 phút

---

## 📊 KẾT QUẢ

### Kết quả trên console:

Sau khi chạy xong, bạn sẽ thấy kết quả của cả 7 phân tích hiển thị trên console:

```
================================================================================
                            RESULTS PREVIEW
================================================================================

=============================== TOP SELLING ====================================
+--------------------+------------------------+----------+-------+
|business_id         |name                    |city      |recent_|
|                    |                        |          |review_|
|                    |                        |          |count  |
+--------------------+------------------------+----------+-------+
|abc123              |Restaurant ABC          |Phoenix   |450    |
|xyz789              |Store XYZ               |Las Vegas |320    |
...
```

### Kết quả lưu file:

Nếu không dùng `--skip-save`, kết quả sẽ được lưu vào `./output/`:

```
output/
├── top_selling/          ← Analysis 1
├── diverse_stores/       ← Analysis 2
├── best_rated/           ← Analysis 3
├── most_positive/        ← Analysis 4
├── peak_hours/           ← Analysis 5
├── top_categories/       ← Analysis 6
└── store_stats/          ← Analysis 7
```

### Đọc kết quả đã lưu:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Đọc Parquet
df = spark.read.parquet("output/top_selling/")
df.show()

# Đọc CSV
df = spark.read.csv("output/top_selling/", header=True)
df.show()
```

---

## ⚙️ TÙY CHỈNH

### Thay đổi cấu hình phân tích:

Edit file `batch_main.py`, dòng ~86:

```python
pipeline.run_all_analyses(config={
    'analysis_1': {'days': 90, 'top_n': 10},      # Thay đổi số ngày, top N
    'analysis_2': {'top_n': 10},
    'analysis_3': {'min_reviews': 10, 'top_n': 10},
    'analysis_4': {'positive_threshold': 4, 'top_n': 10},
    'analysis_6': {'top_n': 20}
})
```

### Chạy riêng 1 phân tích:

```python
from batch_main import run_single_analysis

# Chỉ chạy Analysis 1
result = run_single_analysis(
    analysis_number=1,
    data_path='../data/',
    days=30,
    top_n=5
)
```

### Điều chỉnh Spark memory:

Edit file `batch_configuration.py`, dòng ~28:

```python
.config("spark.driver.memory", "8g")      # Tăng/giảm nếu cần
.config("spark.executor.memory", "4g")
```

---

## 🆘 TROUBLESHOOTING

### Lỗi 1: FileNotFoundError: data/business.json

**Nguyên nhân**: Data files không tồn tại hoặc sai đường dẫn

**Giải pháp**:
```bash
# Kiểm tra data files
ls -la data/

# Đảm bảo có business.json và review.json
# Nếu data ở chỗ khác, chỉ định đường dẫn:
./run_local.sh --data-path /path/to/your/data/
```

### Lỗi 2: Java not found

**Nguyên nhân**: Java chưa cài hoặc không có trong PATH

**Giải pháp**:
```bash
# Cài Java 11
sudo apt install openjdk-11-jdk

# Hoặc download từ: https://adoptium.net/

# Verify
java -version
```

### Lỗi 3: Out of memory / Java heap space

**Nguyên nhân**: Data quá lớn so với RAM

**Giải pháp**:
1. Giảm Spark memory trong `batch_configuration.py`:
```python
.config("spark.driver.memory", "4g")  # Giảm từ 8g
```

2. Lọc data trước khi phân tích:
```python
# Trong batch_load_data.py
review_df = review_df.filter(col("date") >= "2022-01-01")
```

### Lỗi 4: Module 'pyspark' not found

**Nguyên nhân**: PySpark chưa cài đặt

**Giải pháp**:
```bash
pip3 install pyspark==4.0.1 requests

# Verify
python3 -c "import pyspark"
```

### Lỗi 5: Permission denied: ./run_local.sh

**Nguyên nhân**: Script không có quyền execute

**Giải pháp**:
```bash
chmod +x run_local.sh
./run_local.sh
```

### Lỗi 6: JSON parsing error

**Nguyên nhân**: Data file không đúng format JSON

**Giải pháp**:
- Kiểm tra format: mỗi dòng phải là 1 JSON object hợp lệ
- Không được có JSON array `[...]`
- Encoding phải là UTF-8

```bash
# Kiểm tra 5 dòng đầu
head -5 data/business.json

# Mỗi dòng phải bắt đầu bằng { và kết thúc bằng }
```

---

## 🎯 WORKFLOW ĐỀ XUẤT

### Lần đầu chạy:

```bash
# 1. Cài đặt dependencies
pip3 install pyspark==4.0.1 requests

# 2. Chuẩn bị data
mkdir -p data
cp /path/to/your/business.json data/
cp /path/to/your/review.json data/

# 3. Test với dataset nhỏ trước
head -1000 data/business.json > data/business_sample.json
head -10000 data/review.json > data/review_sample.json

# 4. Chạy với sample data
cd Spark_Batch
./run_local.sh --data-path ../data/ --skip-save

# 5. Nếu OK, chạy với full data
./run_local.sh --data-path ../data/
```

### Development workflow:

```bash
# Test 1 analysis
python3 -c "from batch_main import run_single_analysis; run_single_analysis(1, '../data/', days=30)"

# Test full pipeline
./run_local.sh --skip-save

# Chạy và lưu kết quả
./run_local.sh --save-format parquet
```

---

## 📈 TIPS & BEST PRACTICES

### 1. Tăng performance:
```python
# Giảm shuffle partitions cho small data
.config("spark.sql.shuffle.partitions", "10")  # Default: 20

# Cache dataframes thường dùng
business_df.cache()
```

### 2. Debug:
```python
# Bật DEBUG logs
spark.sparkContext.setLogLevel("INFO")

# Xem execution plan
df.explain()
```

### 3. Monitor:
- Spark UI: http://localhost:4040 (trong khi chạy)
- Check memory: `free -h`
- Check CPU: `top`

---

## 📞 HỖ TRỢ

### Nếu gặp vấn đề:

1. **Check logs** - Đọc error message kỹ
2. **Verify data** - Kiểm tra format JSON
3. **Test small** - Chạy với sample data trước
4. **Check resources** - RAM, disk space đủ chưa

### Thông tin hữu ích:

- PySpark docs: https://spark.apache.org/docs/latest/api/python/
- Yelp dataset: https://www.yelp.com/dataset
- Project issues: [GitHub Issues]

---

## ✅ CHECKLIST TRƯỚC KHI CHẠY

- [ ] Python 3.8+ đã cài
- [ ] Java 11+ đã cài
- [ ] PySpark 4.0.1 đã cài (`pip3 install pyspark==4.0.1`)
- [ ] Data files tồn tại trong `data/business.json` và `data/review.json`
- [ ] Data files đúng format (JSON lines)
- [ ] Đủ RAM (~4GB+ free)
- [ ] Đủ disk space (~2GB+)

---

## 🎉 KẾT LUẬN

Bạn đã có một phiên bản đơn giản để chạy 7 phân tích Yelp trên local!

**Các bước chính:**
1. ✅ Cài Python + Java + PySpark
2. ✅ Chuẩn bị data files
3. ✅ Chạy `./run_local.sh`
4. ✅ Xem kết quả!

**Chúc bạn phân tích thành công! 🚀📊**

---

*Version: 1.0.0*
*Last Updated: 2025-12-15*
