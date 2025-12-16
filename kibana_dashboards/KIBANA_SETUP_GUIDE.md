# 📊 KIBANA DASHBOARD SETUP GUIDE

Hướng dẫn tạo và cấu hình Kibana Dashboard cho 9 phân tích Yelp

---

## 🚀 QUICK START

### 1. Khởi động Services

```bash
# Khởi động Elasticsearch + Kibana
docker-compose -f docker-compose-kibana.yml up -d

# Kiểm tra services
docker-compose -f docker-compose-kibana.yml ps

# Xem logs
docker-compose -f docker-compose-kibana.yml logs -f
```

**Chờ ~2-3 phút cho Elasticsearch và Kibana khởi động hoàn toàn**

### 2. Kiểm tra Elasticsearch

```bash
# Test connection
curl http://localhost:9200

# Kết quả mong đợi:
# {
#   "name" : "...",
#   "cluster_name" : "docker-cluster",
#   "version" : { "number" : "8.11.3", ... }
# }
```

### 3. Khởi tạo Indices

```bash
cd Spark_Batch

# Khởi tạo tất cả indices
python3 save_elasticsearch.py

# Hoặc chạy với init flag
python3 batch_main_elasticsearch.py --data-path ./data/ --init-indices
```

### 4. Chạy Batch Analytics & Lưu vào Elasticsearch

```bash
# Chạy toàn bộ pipeline
python3 batch_main_elasticsearch.py --data-path ./data/

# Với Docker
python3 batch_main_elasticsearch.py --data-path ./data/ --es-host elasticsearch --es-port 9200
```

### 5. Truy cập Kibana

Mở browser: **http://localhost:5601**

---

## 📋 TẠO DATA VIEWS (INDEX PATTERNS)

### Bước 1: Truy cập Data Views
1. Mở Kibana: http://localhost:5601
2. Menu **☰** (hamburger icon) → **Management** → **Stack Management**
3. Chọn **Data Views** (hoặc **Index Patterns** ở phiên bản cũ)

### Bước 2: Tạo Data View cho từng Analysis

Click **Create data view** và tạo 9 data views sau:

| # | Data View Name | Index Pattern | Description |
|---|---|---|---|
| 1 | **Analysis 1 - Top Selling** | `yelp-analysis-1-top-selling` | Top selling products theo reviews |
| 2 | **Analysis 2 - User Patterns** | `yelp-analysis-2-user-patterns` | Phân tích hành vi user |
| 3 | **Analysis 3 - Top Users** | `yelp-analysis-3-top-users` | Top users theo số reviews |
| 4 | **Analysis 4 - Category Trends** | `yelp-analysis-4-category-trends` | Xu hướng categories theo thời gian |
| 5 | **Analysis 5 - High Rating** | `yelp-analysis-5-high-rating-low-review` | Businesses ít reviews nhưng rating cao |
| 6 | **Analysis 6 - Geographic** | `yelp-analysis-6-geographic` | Phân bố theo địa lý |
| 7 | **Analysis 7 - Seasonal** | `yelp-analysis-7-seasonal` | Xu hướng theo mùa |
| 8 | **Analysis 8 - Trending** | `yelp-analysis-8-trending` | Businesses đang trending (Window Fns) |
| 9 | **Analysis 9 - Performance** | `yelp-analysis-9-performance-matrix` | Ma trận performance (Pivot/Unpivot) |

**Lưu ý**: Không cần chọn Time Field vì đây là batch analysis.

---

## 📊 TẠO VISUALIZATIONS

### Analysis 1: Top Selling Products

**Visualization Type**: **Horizontal Bar Chart**

**Configuration**:
- **Metrics**: Count
- **Buckets**:
  - **Y-axis**: Terms aggregation on `name.keyword`
  - **Size**: 10
  - **Order**: Metric: Count, Descending
- **Split Series** (optional): Terms on `city.keyword` (top 5)

**Use Case**: Hiển thị top 10 businesses có nhiều reviews nhất trong 15 ngày gần đây.

---

### Analysis 2: User Purchase Patterns

**Visualization Type**: **Data Table**

**Configuration**:
- **Metrics**:
  - Count
  - Average of `avg_stars`
  - Sum of `total_reviews`
- **Buckets**:
  - **Split Rows**: Terms on `review_frequency.keyword`

**Use Case**: Phân tích frequency của user reviews (High/Medium/Low).

---

### Analysis 3: Top Users by Reviews

**Visualization Type**: **Metric** (hoặc **Table**)

**Configuration**:
- **Metrics**:
  - Count of documents
  - Sum of `review_count`
  - Average of `average_stars`
  - Sum of `useful_votes`

**Metric Visualization**: Hiển thị tổng số top users
**Table Visualization**: Show top 20 users với columns: name, review_count, average_stars, useful_votes

**Use Case**: Identify power users trên platform.

---

### Analysis 4: Category Trends Over Time

**Visualization Type**: **Line Chart** hoặc **Area Chart**

**Configuration**:
- **Metrics**: Count hoặc Sum of `review_count`
- **Buckets**:
  - **X-axis**: Date Histogram (combine year + month)
    - Create scripted field nếu cần: `doc['year'].value + '-' + doc['month'].value`
  - **Split Series**: Terms on `category.keyword` (top 5-10 categories)

**Use Case**: Xem xu hướng tăng/giảm của các categories theo thời gian.

**Note**: Có thể cần tạo scripted field để combine year và month thành date field.

---

### Analysis 5: High Rating Low Review Count

**Visualization Type**: **Scatter Plot** hoặc **Data Table**

**Scatter Plot Configuration**:
- **X-axis**: `review_count`
- **Y-axis**: `stars`
- **Size**: Count
- **Color**: Terms on `city.keyword`

**Data Table Configuration**:
- **Columns**: name, city, stars, review_count, categories
- **Sort**: stars descending

**Use Case**: Tìm hidden gems - businesses chất lượng cao nhưng ít người biết.

---

### Analysis 6: Geographic Distribution

**Visualization Type**: **Horizontal Bar Chart** hoặc **Heat Map**

**Bar Chart Configuration**:
- **Metrics**: Sum of `business_count`
- **Buckets**:
  - **Y-axis**: Terms on `city.keyword` (top 15)
  - **Order**: Metric descending
- **Split Chart**: Terms on `state.keyword` (top 5)

**Heat Map Configuration**:
- **Metrics**: Average of `avg_stars`
- **Buckets**:
  - **X-axis**: Terms on `city.keyword`
  - **Y-axis**: Terms on `state.keyword`

**Use Case**: Xem phân bố businesses và quality theo địa lý.

---

### Analysis 7: Seasonal Trends

**Visualization Type**: **Pie Chart** hoặc **Bar Chart**

**Pie Chart Configuration**:
- **Slice Size**: Sum of `review_count`
- **Split Slices**: Terms on `season.keyword`

**Bar Chart Configuration**:
- **Y-axis**: Sum of `review_count`
- **X-axis**: Terms on `month` (sorted by month number)
- **Split Series**: Terms on `season.keyword`

**Use Case**: Xem mùa nào có nhiều reviews nhất, identify peak seasons.

---

### Analysis 8: Trending Businesses (Window Functions)

**Visualization Type**: **Line Chart** với **Multiple Metrics**

**Configuration**:
- **Metrics**:
  - `weekly_count` (line)
  - `prev_week_count` (line)
  - `avg_last_4_weeks` (line)
- **Buckets**:
  - **X-axis**: Date on `week_start`
  - **Split Series**: Terms on `name.keyword` (top 5 trending businesses)

**Additional Metric Visualization**:
- **Type**: Data Table
- **Columns**: name, city, growth_rate, trend_rank
- **Sort**: trend_rank ascending

**Use Case**: Identify businesses có growth rate cao nhất, đang hot trend.

---

### Analysis 9: Category Performance Matrix (Pivot/Unpivot)

**Visualization Type**: **Heat Map**

**Configuration**:
- **Metrics**: Average of `avg_stars`
- **Buckets**:
  - **X-axis**: Terms on `city.keyword` (top 10)
  - **Y-axis**: Terms on `category.keyword` (top 10)

**Alternative - Data Table**:
- **Columns**: category, city, avg_stars, review_count
- **Filters**: Can add filters for specific cities or categories

**Use Case**: Cross-reference performance của categories khác nhau ở các cities khác nhau.

---

## 🎨 TẠO DASHBOARD

### Bước 1: Tạo Dashboard Mới
1. Menu **☰** → **Analytics** → **Dashboard**
2. Click **Create dashboard** hoặc **Create new**

### Bước 2: Add Visualizations
1. Click **Add** button
2. Chọn từng visualization đã tạo ở trên
3. Arrange và resize theo ý muốn

### Bước 3: Layout Recommendations

**Recommended Layout**:

```
┌────────────────────────────────────────────────────────────┐
│  YELP BIG DATA ANALYSIS DASHBOARD                          │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  ┌─────────────────────┐  ┌─────────────────────┐        │
│  │ Analysis 1          │  │ Analysis 8           │        │
│  │ Top Selling         │  │ Trending Businesses  │        │
│  │ (Horizontal Bar)    │  │ (Line Chart)         │        │
│  └─────────────────────┘  └─────────────────────┘        │
│                                                            │
│  ┌─────────────────────┐  ┌─────────────────────┐        │
│  │ Analysis 6          │  │ Analysis 9           │        │
│  │ Geographic Dist.    │  │ Performance Matrix   │        │
│  │ (Heat Map)          │  │ (Heat Map)           │        │
│  └─────────────────────┘  └─────────────────────┘        │
│                                                            │
│  ┌──────────────────────────────────────────────┐        │
│  │ Analysis 4 - Category Trends (Line Chart)    │        │
│  └──────────────────────────────────────────────┘        │
│                                                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ Analysis2│ │Analysis 3│ │Analysis 5│ │Analysis 7│   │
│  │ (Metric) │ │ (Metric) │ │ (Table)  │ │(Pie Chart│   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Bước 4: Add Filters (Optional)
- Add filter controls cho: `city`, `state`, `category`
- Add time range selector nếu có time field
- Add search bar

### Bước 5: Save Dashboard
1. Click **Save** button ở góc trên phải
2. Đặt tên: "Yelp Big Data Analysis Dashboard"
3. Optional: Add description
4. Click **Save**

---

## 🔧 ADVANCED FEATURES

### 1. Drill-down Capabilities
- Enable drill-down trên bar charts để xem chi tiết
- Click vào một bar → filter toàn dashboard

### 2. Time Series Analysis
Nếu muốn add time dimension:
```python
# Add timestamp when saving to ES
from datetime import datetime
df = df.withColumn("@timestamp", lit(datetime.now()))
```

### 3. Custom Filters
Tạo filter controls:
- **Controls** → **Options List** → Select field (e.g., city.keyword)
- Position ở top của dashboard

### 4. Auto-refresh
- Click time picker → Enable auto-refresh
- Set interval (e.g., 30s, 1m, 5m)
- Useful nếu data được update liên tục

### 5. Export & Share
- **Share** → **PDF Reports** → Schedule regular reports
- **Share** → **Embed code** → Embed vào web app
- **Share** → **Permalinks** → Share với team

---

## 🐛 TROUBLESHOOTING

### Problem: "No data views"
**Solution**:
1. Check Elasticsearch: `curl http://localhost:9200/_cat/indices?v`
2. Run pipeline to populate data: `python3 batch_main_elasticsearch.py`
3. Create data views in Kibana

### Problem: "No results found"
**Solution**:
1. Check if data exists: `curl http://localhost:9200/yelp-analysis-1-top-selling/_count`
2. Adjust time range in Kibana (try "Last 5 years" or "No time filter")
3. Check index pattern matches exactly

### Problem: Visualizations not showing data
**Solution**:
1. Click "Inspect" on visualization
2. Check if query is correct
3. Verify field names match (case-sensitive)
4. Try "Refresh field list" in Data View settings

### Problem: Docker containers not starting
**Solution**:
```bash
# Check logs
docker-compose -f docker-compose-kibana.yml logs elasticsearch
docker-compose -f docker-compose-kibana.yml logs kibana

# Restart services
docker-compose -f docker-compose-kibana.yml restart

# Clean restart
docker-compose -f docker-compose-kibana.yml down -v
docker-compose -f docker-compose-kibana.yml up -d
```

### Problem: Out of memory errors
**Solution**:
```yaml
# Edit docker-compose-kibana.yml
elasticsearch:
  environment:
    - "ES_JAVA_OPTS=-Xms1g -Xmx1g"  # Reduce if needed
```

---

## 📊 SAMPLE QUERIES

### Get all indices
```bash
curl http://localhost:9200/_cat/indices?v
```

### Count documents in an index
```bash
curl http://localhost:9200/yelp-analysis-1-top-selling/_count
```

### Search specific index
```bash
curl -X GET "http://localhost:9200/yelp-analysis-8-trending/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "match_all": {}
    },
    "size": 5
  }'
```

### Delete an index (careful!)
```bash
curl -X DELETE "http://localhost:9200/yelp-analysis-1-top-selling"
```

---

## 🎯 BEST PRACTICES

### 1. Index Naming
- ✅ Use lowercase with hyphens: `yelp-analysis-1-top-selling`
- ❌ Avoid: `Yelp_Analysis_1` or `yelpAnalysis1`

### 2. Field Naming
- Use `keyword` type cho fields bạn muốn aggregate (city, state, category)
- Use `text` type cho full-text search fields (name, description)

### 3. Performance
- Limit number of buckets (use "Size" setting wisely)
- Use filters to reduce data size
- Cache frequently used queries

### 4. Dashboard Organization
- Group related visualizations together
- Use consistent color schemes
- Add descriptive titles and descriptions

### 5. Refresh Strategy
- Don't set auto-refresh quá thường xuyên (avoid < 30s)
- Manual refresh cho analysis dashboards
- Auto-refresh chỉ cho real-time monitoring

---

## 📚 NEXT STEPS

### 1. Advanced Visualizations
- Tạo Vega visualizations cho custom charts
- Use Canvas cho infographic-style dashboards
- Explore Maps cho geographic visualizations

### 2. Alerting
- Set up alerts cho anomaly detection
- Email notifications khi có threshold violations

### 3. Machine Learning
- Use Kibana ML features cho anomaly detection
- Forecasting trends

### 4. Integration
- Embed dashboards vào web applications
- API integration với external systems

---

## 🔗 RESOURCES

- **Kibana Documentation**: https://www.elastic.co/guide/en/kibana/current/index.html
- **Elasticsearch Guide**: https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
- **Visualization Examples**: https://www.elastic.co/guide/en/kibana/current/dashboard.html

---

**Happy Visualizing! 📊🎉**

*Last Updated: 2025-12-16*
