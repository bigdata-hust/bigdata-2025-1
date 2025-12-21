# ⚡ QUICKSTART - 5 phút để chạy

## 🎯 Mục tiêu
Chạy 7 phân tích Yelp trong 5 phút!

---

## 📝 Các bước (Ubuntu 16GB RAM)

### Bước 1: Cài dependencies (1 phút)
```bash
# Cài Java (nếu chưa có)
sudo apt install openjdk-11-jdk -y

# Cài PySpark
pip3 install pyspark==4.0.1 requests
```

### Bước 2: Chuẩn bị data (1 phút)

**Nếu đã có data:**
```bash
cd /home/user/bigdata-2025-1

# Tạo thư mục data
mkdir -p data

# Copy data files của bạn
cp /path/to/your/business.json data/
cp /path/to/your/review.json data/
```

**Nếu chưa có data (tạo sample):**
```bash
cd /home/user/bigdata-2025-1/Spark_Batch

# Tạo sample data tự động
python3 create_sample_data.py
```

### Bước 3: Chạy phân tích! (2-3 phút)
```bash
cd /home/user/bigdata-2025-1/Spark_Batch

# Chạy pipeline
./run_local.sh
```

### Bước 4: Xem kết quả
Kết quả sẽ hiển thị trên console và lưu trong `./output/`

---

## 🎉 DONE!

Bạn đã chạy xong 7 phân tích:
- ✅ Top Selling Products
- ✅ Diverse Stores
- ✅ Best Rated
- ✅ Positive Reviews
- ✅ Peak Hours
- ✅ Top Categories
- ✅ Store Statistics

---

## 📋 Troubleshooting nhanh

**Lỗi: java not found**
```bash
sudo apt install openjdk-11-jdk -y
```

**Lỗi: pyspark not found**
```bash
pip3 install pyspark==4.0.1 requests
```

**Lỗi: data files not found**
```bash
# Tạo sample data
cd Spark_Batch
python3 create_sample_data.py
```

---

## 📖 Chi tiết hơn?
Đọc [README_BATCH.md](README_BATCH.md) để biết thêm thông tin!
