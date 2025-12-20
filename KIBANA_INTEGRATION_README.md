# 📊 YELP BIG DATA ANALYSIS - KIBANA INTEGRATION

Hệ thống phân tích dữ liệu Yelp với **Batch Processing + Elasticsearch + Kibana Visualization**

---

## 🎯 TỔNG QUAN

### Kiến trúc Hybrid

Kết hợp kiến trúc từ 2 nhánh:
- **Nhánh hiện tại**: Batch processing với 9 analyses (7 basic + 2 advanced)
- **Nhánh "hai"**: Elasticsearch + Kibana visualization infrastructure

### Components

```
┌─────────────────────────────────────────────────────────┐
│                  YELP ANALYTICS PIPELINE                │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────┐                                   │
│  │  Data Sources   │                                   │
│  │  (JSON Files)   │                                   │
│  └────────┬────────┘                                   │
│           │                                            │
│           ▼                                            │
│  ┌─────────────────┐        ┌──────────────────┐     │
│  │  PySpark Batch  │───────►│ Elasticsearch    │     │
│  │  Processing     │        │ (9 indices)      │     │
│  │  (9 Analyses)   │        └────────┬─────────┘     │
│  └─────────────────┘                 │               │
│           │                           │               │
│           │                           ▼               │
│           ▼                  ┌──────────────────┐     │
│  ┌─────────────────┐        │    Kibana        │     │
│  │  CSV Outputs    │        │  (Dashboards)    │     │
│  └─────────────────┘        └──────────────────┘     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## ⚡ QUICK START (3 bước)

### Bước 1: Khởi động Elasticsearch + Kibana

```bash
# Khởi động services
docker-compose -f docker-compose-kibana.yml up -d

# Chờ services ready (~2-3 phút)
docker-compose -f docker-compose-kibana.yml logs -f
```

**Kiểm tra**:
- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601

### Bước 2: Chạy Batch Analytics + Lưu vào Elasticsearch

```bash
cd Spark_Batch

# Khởi tạo indices và chạy pipeline
python3 batch_main_elasticsearch.py \
  --data-path ./data/ \
  --init-indices
```

**Output**:
```
================================================================================
                YELP BIG DATA ANALYSIS WITH KIBANA
                     Batch Mode + Elasticsearch Integration
                          Run Time: 2025-12-16 11:00:00
================================================================================

Initializing Elasticsearch indices...
✓ Index 'yelp-analysis-1-top-selling' created successfully
✓ Index 'yelp-analysis-2-user-patterns' created successfully
...
✓ All indices initialized successfully!

Initializing pipeline...
  Data Path: ./data/
  Output Path: ./output_elasticsearch/
  Elasticsearch: localhost:9200

✓ Connected to Elasticsearch 8.11.3
✓ Elasticsearch connected successfully

================================================================================
DATA LOADING PHASE
================================================================================
Loading business data from ./data/
✓ Loaded 150,346 businesses
...

================================================================================
               RUNNING ALL ANALYSES WITH ELASTICSEARCH
================================================================================

[1/9] Running Analysis 1: Top Selling Products...
✓ Analysis 1 completed
✓ Saved to Elasticsearch: yelp-analysis-1-top-selling

[2/9] Running Analysis 2: User Purchase Patterns...
...

================================================================================
                     ✓ ALL ANALYSES COMPLETED
================================================================================

✓ Analyses completed: 9
✓ CSV outputs: ./output_elasticsearch/
✓ Elasticsearch indices: 9
✓ Kibana dashboard: http://localhost:5601
```

### Bước 3: Tạo Kibana Dashboard

Xem hướng dẫn chi tiết: **[kibana_dashboards/KIBANA_SETUP_GUIDE.md](kibana_dashboards/KIBANA_SETUP_GUIDE.md)**

**Tóm tắt nhanh**:
1. Mở Kibana: http://localhost:5601
2. Tạo 9 Data Views (index patterns)
3. Tạo visualizations cho mỗi analysis
4. Combine vào 1 dashboard

---

## 📁 CẤU TRÚC DỰ ÁN

```
bigdata-2025-1/
│
├── docker-compose-kibana.yml          ← Docker services config
│
├── Spark_Batch/
│   ├── batch_main_elasticsearch.py ⭐ ← Main entry point với ES integration
│   ├── save_elasticsearch.py       ⭐ ← ES saver module
│   │
│   ├── batch_main_v2.py              ← Original main (CSV only)
│   ├── batch_pipeline.py             ← Pipeline orchestrator
│   ├── batch_analytics.py            ← 7 analyses cơ bản
│   ├── batch_analytics_advanced.py   ← 2 analyses nâng cao
│   ├── batch_udf.py                  ← UDF library
│   └── ...
│
├── kibana_dashboards/
│   └── KIBANA_SETUP_GUIDE.md      ⭐ ← Chi tiết setup Kibana
│
├── KIBANA_INTEGRATION_README.md   ⭐ ← File này
│
└── data/
    ├── business.json
    ├── review.json
    └── user.json
```

⭐ = Files mới cho Kibana integration

---

## 📊 9 ANALYSES VÀ ELASTICSEARCH INDICES

| Analysis | Index Name | Visualization Type | Key Metrics |
|---|---|---|---|
| **1. Top Selling Products** | `yelp-analysis-1-top-selling` | Horizontal Bar Chart | review_count, avg_stars, name |
| **2. User Purchase Patterns** | `yelp-analysis-2-user-patterns` | Data Table | total_reviews, avg_stars, frequency |
| **3. Top Users by Reviews** | `yelp-analysis-3-top-users` | Metric / Table | review_count, useful_votes |
| **4. Category Trends** | `yelp-analysis-4-category-trends` | Line Chart | category, year, month, count |
| **5. High Rating Low Review** | `yelp-analysis-5-high-rating-low-review` | Scatter Plot | stars vs review_count |
| **6. Geographic Distribution** | `yelp-analysis-6-geographic` | Heat Map | city, state, business_count |
| **7. Seasonal Trends** | `yelp-analysis-7-seasonal` | Pie Chart | season, review_count |
| **8. Trending Businesses** ⭐ | `yelp-analysis-8-trending` | Line Chart | growth_rate, weekly_count |
| **9. Performance Matrix** ⭐ | `yelp-analysis-9-performance-matrix` | Heat Map | category × city matrix |

⭐ = Advanced analyses với Window Functions & Pivot/Unpivot

---

## 🚀 CÁC DEPLOYMENT MODES

### Mode 1: Local Development (Recommended để bắt đầu)

```bash
# 1. Start Elasticsearch + Kibana
docker-compose -f docker-compose-kibana.yml up -d

# 2. Run Spark locally (không qua Docker)
cd Spark_Batch
python3 batch_main_elasticsearch.py --data-path ./data/

# 3. Access Kibana
open http://localhost:5601
```

**Ưu điểm**:
- Debug dễ dàng
- Không cần build Docker image cho Spark
- Chạy nhanh hơn

### Mode 2: Full Docker (Production-ready)

```bash
# TODO: Uncomment spark-batch service trong docker-compose-kibana.yml
# và build Docker image

docker-compose -f docker-compose-kibana.yml up -d --build
```

**Ưu điểm**:
- Production-ready
- Isolated environment
- Easy deployment

### Mode 3: Hybrid with Streaming (Advanced)

Kết hợp với kiến trúc streaming từ nhánh "hai":
```bash
# Merge features từ cả 2 nhánh
# - Batch analytics từ nhánh hiện tại
# - Kafka streaming từ nhánh "hai"
# - Unified Elasticsearch + Kibana
```

---

## 🔧 CONFIGURATION

### Elasticsearch Settings

File: `docker-compose-kibana.yml`

```yaml
elasticsearch:
  environment:
    - "ES_JAVA_OPTS=-Xms2g -Xmx2g"  # Heap size
    - discovery.type=single-node
    - xpack.security.enabled=false   # Disable security for dev
```

**Tuning**:
- RAM < 8GB: Set `-Xms1g -Xmx1g`
- RAM >= 16GB: Set `-Xms4g -Xmx4g`

### Spark Settings

File: `Spark_Batch/batch_configuration.py`

```python
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "4g")
```

**Tuning dựa trên RAM**:
- 8GB RAM: driver=4g, executor=2g
- 16GB RAM: driver=8g, executor=4g
- 32GB+ RAM: driver=12g, executor=8g

### Environment Variables

```bash
# For Docker deployment
export ELASTICSEARCH_HOST=elasticsearch  # or localhost
export ELASTICSEARCH_PORT=9200
export DATA_PATH=/app/data
```

---

## 📖 DOCUMENTATION MAP

### Để bắt đầu:
1. **README.md này** - Overview và quick start
2. **Spark_Batch/00_START_HERE.md** - Code structure
3. **kibana_dashboards/KIBANA_SETUP_GUIDE.md** - Kibana setup chi tiết

### Để hiểu code:
1. **Spark_Batch/PROJECT_STRUCTURE.md** - Cấu trúc chi tiết 13 files
2. **Spark_Batch/ARCHITECTURE_DIAGRAM.md** - Sơ đồ kiến trúc
3. **Spark_Batch/LOCAL_TEST_GUIDE.md** - Test features

### API Reference:
- **save_elasticsearch.py** - API documentation trong code
- **batch_main_elasticsearch.py** - Usage examples

---

## 🐛 TROUBLESHOOTING

### Problem: "Cannot connect to Elasticsearch"

**Kiểm tra**:
```bash
# Check if ES is running
curl http://localhost:9200

# Check Docker containers
docker-compose -f docker-compose-kibana.yml ps

# Check logs
docker-compose -f docker-compose-kibana.yml logs elasticsearch
```

**Solution**:
```bash
# Restart services
docker-compose -f docker-compose-kibana.yml restart

# Or full restart
docker-compose -f docker-compose-kibana.yml down
docker-compose -f docker-compose-kibana.yml up -d
```

### Problem: "No data in Kibana"

**Kiểm tra**:
```bash
# Check if indices exist
curl http://localhost:9200/_cat/indices?v

# Check document count
curl http://localhost:9200/yelp-analysis-1-top-selling/_count
```

**Solution**:
```bash
# Re-run pipeline
cd Spark_Batch
python3 batch_main_elasticsearch.py --data-path ./data/ --init-indices
```

### Problem: OutOfMemoryError

**Solution 1: Giảm Elasticsearch heap**
```yaml
# docker-compose-kibana.yml
environment:
  - "ES_JAVA_OPTS=-Xms1g -Xmx1g"
```

**Solution 2: Giảm Spark memory**
```python
# batch_configuration.py
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "2g")
```

**Solution 3: Limit data size**
```bash
# Process smaller data subset
python3 batch_main_elasticsearch.py --data-path ./data_small/
```

### Problem: Port conflicts (9200, 5601 already in use)

**Solution**:
```yaml
# Change ports in docker-compose-kibana.yml
elasticsearch:
  ports:
    - "9201:9200"  # Change from 9200

kibana:
  ports:
    - "5602:5601"  # Change from 5601
```

Then update code:
```bash
python3 batch_main_elasticsearch.py --es-port 9201
```

---

## 🎓 ADVANCED TOPICS

### 1. Real-time Updates

Enable auto-refresh trong Kibana:
```
Time Picker → Auto-refresh → 30s
```

Chạy pipeline định kỳ:
```bash
# Cron job example (every hour)
0 * * * * cd /path/to/Spark_Batch && python3 batch_main_elasticsearch.py --data-path ./data/
```

### 2. Custom Visualizations

Tạo Vega visualizations cho advanced charts:
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {
    "url": {
      "index": "yelp-analysis-8-trending",
      "body": { "size": 100 }
    }
  },
  "mark": "line",
  "encoding": {
    "x": {"field": "week_start", "type": "temporal"},
    "y": {"field": "growth_rate", "type": "quantitative"}
  }
}
```

### 3. Machine Learning Integration

Use Kibana ML for anomaly detection:
```
Analytics → Machine Learning → Anomaly Detection
→ Create job → Select index → Configure metrics
```

### 4. Alerting

Set up alerts cho anomaly:
```
Stack Management → Rules and Connectors → Create rule
→ Elasticsearch query → Define threshold → Set actions
```

---

## 📈 PERFORMANCE TIPS

### 1. Batch Size Optimization

```python
# save_elasticsearch.py
# Adjust partition size for better performance
df = df.repartition(10)  # 10 partitions
df.foreachPartition(send_partition)
```

### 2. Index Optimization

```bash
# Force merge để optimize storage
curl -X POST "localhost:9200/yelp-analysis-*/_forcemerge?max_num_segments=1"
```

### 3. Query Performance

```bash
# Create alias cho multiple indices
curl -X POST "localhost:9200/_aliases" -H 'Content-Type: application/json' -d'
{
  "actions": [
    {"add": {"index": "yelp-analysis-*", "alias": "yelp-all"}}
  ]
}
'
```

### 4. Resource Monitoring

```bash
# Check cluster health
curl http://localhost:9200/_cluster/health?pretty

# Check node stats
curl http://localhost:9200/_nodes/stats?pretty
```

---

## 🔗 SO SÁNH VỚI NHÁNH "HAI"

| Feature | Nhánh "hai" | Nhánh hiện tại (Integrated) |
|---|---|---|
| **Processing Mode** | Streaming (Kafka) | Batch |
| **Analyses** | 8 analyses | 9 analyses (7 basic + 2 advanced) |
| **Advanced Features** | Sentiment analysis | Window Functions, Pivot/Unpivot, UDF Library |
| **Elasticsearch** | ✅ | ✅ |
| **Kibana** | ✅ | ✅ |
| **Docker Setup** | Full stack (Kafka, HDFS, Spark) | Lightweight (ES + Kibana only) |
| **Deployment Complexity** | High (nhiều services) | Medium (ES + Kibana + Spark local) |
| **Use Case** | Real-time streaming | Batch analytics, periodic updates |

**Best of Both Worlds**:
- Use **nhánh hiện tại** cho: Batch analytics, complex analyses, development
- Use **nhánh "hai"** cho: Real-time streaming, live updates
- **Merge** cả 2 cho: Hybrid architecture với cả batch và streaming

---

## 🎯 NEXT STEPS

### Để tiếp tục phát triển:

1. **Merge Streaming Features**:
   ```bash
   # Merge Kafka + streaming từ nhánh "hai"
   git checkout claude/review-project-structure-pfDJE
   git merge hai --no-commit
   # Resolve conflicts
   ```

2. **Add More Analyses**:
   - Sentiment analysis (từ nhánh "hai")
   - Time series forecasting
   - Recommendation system

3. **Production Deployment**:
   - Setup Kubernetes for scaling
   - Add monitoring (Prometheus + Grafana)
   - Implement CI/CD pipeline

4. **Advanced Visualizations**:
   - Custom Vega charts
   - Canvas for infographics
   - Maps cho geographic data

---

## 📞 SUPPORT

### Documentation:
- **Kibana Setup**: `kibana_dashboards/KIBANA_SETUP_GUIDE.md`
- **Code Structure**: `Spark_Batch/PROJECT_STRUCTURE.md`
- **Architecture**: `Spark_Batch/ARCHITECTURE_DIAGRAM.md`

### Quick Commands:
```bash
# Start services
docker-compose -f docker-compose-kibana.yml up -d

# Run pipeline
cd Spark_Batch && python3 batch_main_elasticsearch.py --data-path ./data/

# Stop services
docker-compose -f docker-compose-kibana.yml down

# Clean everything
docker-compose -f docker-compose-kibana.yml down -v
```

---

**Happy Analyzing and Visualizing! 📊🎉**

*Last Updated: 2025-12-16*
*Version: 2.0 - Kibana Integration*
*Branch: claude/review-project-structure-pfDJE*
