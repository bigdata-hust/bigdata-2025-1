# 🚀 HƯỚNG DẪN SỬ DỤNG NHANH

## 📁 Cấu trúc thư mục BẮT BUỘC

```
your-project/
├── processed_data/              ← Dữ liệu CSV của bạn
│   ├── business.csv
│   ├── user.csv
│   └── review_combined_1.csv
│
└── spark-elasticsearch-kibana/  ← Code này (sau khi giải nén)
    └── ...
```

**⚠️ QUAN TRỌNG**: 
- Thư mục `processed_data` phải nằm **NGANG HÀNG** với `spark-elasticsearch-kibana`
- **KHÔNG** đặt `processed_data` bên trong `spark-elasticsearch-kibana`

---

## ⚡ 3 BƯỚC CHẠY NHANH

### Bước 1: Setup môi trường (1 lần duy nhất)

#### Trên Linux/Mac:
```bash
cd spark-elasticsearch-kibana
chmod +x setup.sh
./setup.sh
```

#### Trên Windows:
```bash
cd spark-elasticsearch-kibana
pip install -r requirements.txt
docker-compose up -d
```

### Bước 2: Chạy Pipeline

```bash
python main.py
```

**Thời gian chạy**: 5-15 phút (tùy kích thước data)

### Bước 3: Xem kết quả trong Kibana

1. Mở browser: http://localhost:5601
2. Vào **Stack Management** > **Index Patterns** > **Create index pattern**
3. Tạo pattern: `yelp-*` (time field: `timestamp`)
4. Vào **Discover** để xem data
5. Vào **Visualize** > **Dashboard** để tạo dashboard

---

## 📊 Pipeline sẽ làm gì?

1. ✅ Đọc 3 file CSV từ `../processed_data/`
2. ✅ Validate dữ liệu
3. ✅ Chạy 7 loại phân tích:
   - Top sản phẩm bán chạy
   - Cửa hàng đa dạng nhất  
   - Đánh giá tốt nhất
   - Review tích cực nhất
   - Thời gian cao điểm
   - Top categories
   - Thống kê tổng hợp
4. ✅ Export 7 indices vào Elasticsearch
5. ✅ Hiển thị kết quả

---

## 🔍 Kiểm tra nhanh

### Kiểm tra dữ liệu có đúng chỗ không?
```bash
ls ../processed_data/
# Phải thấy: business.csv, user.csv, review_combined_1.csv
```

### Kiểm tra Docker đã chạy chưa?
```bash
docker-compose ps
# Phải thấy elasticsearch và kibana đang UP
```

### Kiểm tra Elasticsearch
```bash
curl http://localhost:9200
# Phải thấy response JSON
```

### Kiểm tra indices sau khi chạy
```bash
curl http://localhost:9200/_cat/indices?v | grep yelp
# Phải thấy 7 indices: yelp-*
```

---

## 🆘 Gặp lỗi?

### Lỗi: "FileNotFoundError: ../processed_data/business.csv"

**Nguyên nhân**: Thư mục data không đúng chỗ

**Giải pháp**:
```bash
# Kiểm tra cấu trúc
pwd
ls ..

# Đảm bảo thấy:
# processed_data/
# spark-elasticsearch-kibana/
```

### Lỗi: "Connection refused to localhost:9200"

**Nguyên nhân**: Elasticsearch chưa chạy

**Giải pháp**:
```bash
docker-compose up -d
docker-compose ps
docker-compose logs elasticsearch
```

### Lỗi: "OutOfMemoryError"

**Giải pháp**: Giảm memory requirements
```yaml
# Edit docker-compose.yml
ES_JAVA_OPTS=-Xms1g -Xmx1g  # Từ 2g xuống 1g
```

---

## 📝 Cấu trúc file CSV yêu cầu

### business.csv
```
business_id,name,city,state,categories,stars,review_count,is_open,latitude,longitude
abc123,Restaurant A,Phoenix,AZ,"Food, Asian",4.5,100,1,33.45,-112.07
...
```

### review_combined_1.csv
```
review_id,user_id,business_id,stars,useful,date,text
xyz789,user1,abc123,5,10,2024-01-15,Great food!
...
```

### user.csv
```
user_id,name,review_count,yelping_since,useful,fans,average_stars
user1,John Doe,50,2020-01-01,100,5,4.2
...
```

---

## 🎯 Tùy chỉnh nhanh

### Thay đổi số lượng kết quả

Edit `main.py`, tìm `analysis_config`:
```python
analysis_config = {
    'analysis_1': {'days': 90, 'top_n': 10},  # Đổi thành 180, 20
    ...
}
```

### Thay đổi đường dẫn data

Edit `.env`:
```bash
DATA_PATH=../processed_data/  # Đổi thành path của bạn
```

### Chỉ chạy một vài analyses

Comment out trong `main.py`, function `run_analysis()`:
```python
# results['top_selling'] = ...  # Comment để skip
```

---

## 💻 Commands hữu ích

```bash
# Khởi động services
docker-compose up -d

# Dừng services  
docker-compose down

# Xem logs
docker-compose logs -f elasticsearch
docker-compose logs -f kibana

# Restart services
docker-compose restart

# Xóa toàn bộ data và restart
docker-compose down -v
docker-compose up -d

# Kiểm tra indices
curl http://localhost:9200/_cat/indices?v

# Đếm documents trong index
curl http://localhost:9200/yelp-top-selling/_count

# Xem sample document
curl http://localhost:9200/yelp-top-selling/_search?size=1&pretty
```

---

## 📊 Elasticsearch Indices tạo ra

| Index | Mô tả | Documents |
|-------|-------|-----------|
| yelp-top-selling | Top sản phẩm bán chạy | ~10 |
| yelp-diverse-stores | Cửa hàng đa dạng | ~10 |
| yelp-best-rated | Đánh giá tốt nhất | ~10 |
| yelp-positive-reviews | Review tích cực | ~10 |
| yelp-peak-hours | Thời gian cao điểm | Variable |
| yelp-top-categories | Top danh mục | ~20 |
| yelp-store-stats | Thống kê tổng hợp | All businesses |

---

## 🎨 Tạo Dashboard trong Kibana

### Bước 1: Tạo Index Pattern
1. Mở http://localhost:5601
2. Menu > Stack Management > Index Patterns
3. Create index pattern: `yelp-top-selling*`
4. Time field: `timestamp`
5. Create

### Bước 2: Khám phá dữ liệu
1. Menu > Discover
2. Chọn index pattern vừa tạo
3. Xem dữ liệu đã import

### Bước 3: Tạo Visualization
1. Menu > Visualize > Create visualization
2. Chọn type: Data Table / Bar Chart / Line...
3. Chọn index pattern
4. Configure metrics và buckets
5. Save

### Bước 4: Tạo Dashboard
1. Menu > Dashboard > Create dashboard
2. Add các visualizations đã tạo
3. Arrange layout
4. Save

---

## 📖 Xem thêm

- **README.md**: Tài liệu đầy đủ
- **Troubleshooting**: Các lỗi phổ biến
- **Kibana examples**: Mẫu visualizations

---

## ✅ Checklist hoàn thành

- [ ] Thư mục `processed_data` đúng vị trí
- [ ] 3 file CSV có dữ liệu
- [ ] Docker đã cài đặt
- [ ] Docker Compose đã cài đặt
- [ ] Python 3.8+ đã cài đặt
- [ ] Đã chạy `setup.sh` hoặc cài requirements.txt
- [ ] Docker containers đang chạy
- [ ] Elasticsearch accessible (curl localhost:9200)
- [ ] Kibana accessible (curl localhost:5601)
- [ ] Đã chạy `python main.py` thành công
- [ ] 7 indices đã được tạo
- [ ] Có thể xem data trong Kibana

---

## 🎉 Thành công!

Nếu checklist trên đều ✅, bạn đã setup thành công!

Bây giờ bạn có thể:
- ✅ Phân tích dữ liệu Yelp
- ✅ Query real-time từ Elasticsearch  
- ✅ Tạo dashboards đẹp trong Kibana
- ✅ Export kết quả sang CSV/Parquet
- ✅ Tùy chỉnh analyses theo nhu cầu

**Chúc mừng! 🚀📊**

---

*Nếu cần hỗ trợ, xem file README.md hoặc TROUBLESHOOTING.md*
