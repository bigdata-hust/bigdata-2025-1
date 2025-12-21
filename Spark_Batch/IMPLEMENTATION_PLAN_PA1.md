# 🚀 PHƯƠNG ÁN 1 - IMPLEMENTATION PLAN & LOCAL TEST GUIDE

**Mục tiêu:** Triển khai 4 kỹ năng Spark nâng cao và test trên local

**Timeline:** 2-3 ngày

**Score:** 42% → 64%

---

## 📋 MỤC LỤC

1. [Tổng quan triển khai](#1-tổng-quan-triển-khai)
2. [Yêu cầu môi trường](#2-yêu-cầu-môi-trường)
3. [Chi tiết từng phần](#3-chi-tiết-từng-phần)
4. [Cách test local](#4-cách-test-local)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. TỔNG QUAN TRIỂN KHAI

### 🎯 4 Kỹ năng sẽ thêm:

| # | Kỹ năng | File mới/sửa | Test độc lập? | Thời gian |
|---|---------|--------------|---------------|-----------|
| 1 | **Window Functions** | `batch_analytics_advanced.py` | ✅ Có | 1 ngày |
| 2 | **Broadcast Join** | Refactor 7 analyses cũ | ✅ Có | 0.5 ngày |
| 3 | **Pivot/Unpivot** | `batch_analytics_advanced.py` | ✅ Có | 0.5 ngày |
| 4 | **UDF/Pandas UDF** | `batch_udf.py` | ✅ Có | 1 ngày |

### 📁 Cấu trúc files mới:

```
Spark_Batch/
├── batch_analytics.py                  (hiện tại - sẽ refactor)
├── batch_analytics_advanced.py         (MỚI - Analysis 8, 9)
├── batch_udf.py                        (MỚI - UDF collection)
├── batch_main_v2.py                    (MỚI - Run all 9 analyses)
├── test_local_features.py              (MỚI - Test script)
└── LOCAL_TEST_GUIDE.md                 (MỚI - Hướng dẫn test)
```

### 🔄 Workflow triển khai:

```
Step 1: Tạo UDF library
   ↓
Step 2: Implement Window Functions (Analysis 8)
   ↓ (Test độc lập)
Step 3: Implement Pivot/Unpivot (Analysis 9)
   ↓ (Test độc lập)
Step 4: Refactor 7 analyses với Broadcast Join
   ↓ (Test compare kết quả)
Step 5: Tích hợp tất cả vào main_v2.py
   ↓
Step 6: Full test với real data
```

---

## 2. YÊU CẦU MÔI TRƯỜNG

### ✅ Đã có (từ Spark_Batch):
- Python 3.8+
- Java 11+
- PySpark 4.0.1
- Data files (business.json, review.json)

### ➕ Thêm cho Phương án 1:
```bash
# Cài thêm cho Pandas UDF
pip install pandas==2.0.3 pyarrow==12.0.0
```

### 💾 Dung lượng cần thiết:
- Code mới: ~5KB
- Sample data (nếu test): ~10MB
- Output results: ~50MB

---

## 3. CHI TIẾT TỪNG PHẦN

### 📊 3.1. WINDOW FUNCTIONS (Priority 1)

#### 🎯 Mục tiêu:
Tạo Analysis 8: **Trending Businesses** - Phát hiện businesses đang tăng trưởng

#### 🔧 Kỹ năng thể hiện:
- `lag()`, `lead()` - So sánh với tuần trước/sau
- `avg()` over window - Moving average 4 tuần
- `dense_rank()` - Ranking trong tuần
- `rowsBetween()` - Window frame specification
- `partitionBy()` - Partition theo business_id

#### 💡 Ý tưởng:
```
Input: review_df
  ↓
Group by business_id + week (sliding window 7 days)
  ↓
Calculate: weekly_count, prev_week_count
  ↓
Calculate: growth_rate = (current - prev) / prev
  ↓
Calculate: avg_last_4_weeks (moving average)
  ↓
Rank businesses by growth_rate
  ↓
Output: Top trending businesses
```

#### 📝 Pseudo code:
```python
# 1. Group by week
weekly_reviews = review_df.groupBy(
    "business_id",
    window("review_date", "7 days")
).agg(count("review_id"))

# 2. Window functions
windowSpec = Window.partitionBy("business_id").orderBy("week_start")

trending = weekly_reviews.withColumn(
    "prev_week_count", lag("weekly_count", 1).over(windowSpec)
).withColumn(
    "growth_rate", (col("weekly_count") - col("prev_week_count")) / col("prev_week_count")
).withColumn(
    "avg_last_4_weeks", avg("weekly_count").over(windowSpec.rowsBetween(-3, 0))
).withColumn(
    "rank", dense_rank().over(Window.orderBy(desc("growth_rate")))
)
```

#### 🧪 Test độc lập:
```bash
# Test chỉ Analysis 8
python3 test_local_features.py --test window

# Expected output:
# - Top 10 businesses with highest growth rate
# - Các columns: business_id, name, weekly_count, growth_rate, avg_last_4_weeks, rank
```

---

### 🔗 3.2. BROADCAST JOIN (Priority 2)

#### 🎯 Mục tiêu:
Refactor 7 analyses hiện tại để dùng explicit broadcast join

#### 🔧 Kỹ năng thể hiện:
- `broadcast()` function explicit
- Optimization cho small table joins
- Before/after performance comparison

#### 💡 Thay đổi:

**BEFORE (implicit):**
```python
result = top_candidates.join(
    business_df.select("business_id", "name", "city"),
    "business_id"
)
```

**AFTER (explicit broadcast):**
```python
from pyspark.sql.functions import broadcast

result = top_candidates.join(
    broadcast(business_df.select("business_id", "name", "city")),
    "business_id"
)
```

#### 📂 Files cần sửa:
- `batch_analytics.py`:
  - Analysis 1: line ~51-53
  - Analysis 3: line ~137-142
  - Analysis 4: line ~203-208
  - Analysis 6: line ~276-279
  - Analysis 7: line ~316

#### 🧪 Test độc lập:
```bash
# Test broadcast join performance
python3 test_local_features.py --test broadcast

# Expected:
# - Kết quả giống hệt trước (correctness)
# - Thời gian chạy nhanh hơn 10-30% (performance)
# - Spark UI confirm broadcast join (check physical plan)
```

---

### 🔄 3.3. PIVOT/UNPIVOT (Priority 3)

#### 🎯 Mục tiêu:
Tạo Analysis 9: **Category Performance Matrix** - Pivot categories vs cities

#### 🔧 Kỹ năng thể hiện:
- `pivot()` - Wide format transformation
- `unpivot()` / `stack()` - Long format transformation
- Cross-tabulation analysis

#### 💡 Ý tưởng:
```
Input: business_df + review_df
  ↓
Explode categories
  ↓
Join with reviews
  ↓
Aggregate: avg_stars, review_count per (category, city)
  ↓
Pivot: categories as rows, cities as columns
  ↓
Output: Performance matrix
```

#### 📝 Pseudo code:
```python
# 1. Explode and join
df = business_df.withColumn("category", explode(split(col("categories"), ",")))
joined = df.join(review_df, "business_id")

# 2. Aggregate
agg_df = joined.groupBy("category", "city").agg(
    avg("stars").alias("avg_stars"),
    count("review_id").alias("review_count")
)

# 3. Pivot
pivoted = agg_df.groupBy("category").pivot("city").agg(
    first("avg_stars"),
    first("review_count")
)

# Bonus: Unpivot back
unpivoted = pivoted.select("category", expr("stack(N, ...)"))
```

#### 🧪 Test độc lập:
```bash
# Test pivot/unpivot
python3 test_local_features.py --test pivot

# Expected:
# - Pivot: Wide table (categories x cities)
# - Unpivot: Long table (category, city, avg_stars, review_count)
```

---

### 🎨 3.4. UDF / PANDAS UDF (Priority 4)

#### 🎯 Mục tiêu:
Tạo UDF library với 4 functions:

1. **categorize_rating** (Regular UDF) - Phân loại rating
2. **sentiment_score** (Pandas UDF) - Tính sentiment score
3. **extract_keywords** (Pandas UDF) - Extract keywords từ text
4. **is_weekend** (Regular UDF) - Check weekend

#### 🔧 Kỹ năng thể hiện:
- Regular UDF với `@udf` decorator
- Pandas UDF (vectorized) với `@pandas_udf`
- Performance comparison: Regular vs Pandas UDF

#### 💡 Functions:

**1. categorize_rating (Regular UDF):**
```python
@udf(returnType=StringType())
def categorize_rating(stars):
    """Categorize rating: Excellent, Good, Average, Poor"""
    if stars >= 4.5: return "Excellent"
    elif stars >= 3.5: return "Good"
    elif stars >= 2.5: return "Average"
    else: return "Poor"
```

**2. sentiment_score (Pandas UDF - Fast!):**
```python
@pandas_udf(FloatType())
def sentiment_score(text: pd.Series) -> pd.Series:
    """Calculate sentiment score from review text"""
    positive_words = ['great', 'excellent', 'amazing', 'love', 'best']
    negative_words = ['bad', 'terrible', 'worst', 'hate', 'awful']

    def score(t):
        pos = sum(t.lower().count(w) for w in positive_words)
        neg = sum(t.lower().count(w) for w in negative_words)
        return pos / (pos + neg + 1)

    return text.apply(score)
```

**3. extract_keywords (Pandas UDF):**
```python
@pandas_udf(StringType())
def extract_keywords(text: pd.Series) -> pd.Series:
    """Extract top keywords from text"""
    import re

    def extract(t):
        words = re.findall(r'\b[a-z]{4,}\b', t.lower())
        # Return top 5 most common
        from collections import Counter
        top = Counter(words).most_common(5)
        return ', '.join([w for w, c in top])

    return text.apply(extract)
```

**4. is_weekend (Regular UDF):**
```python
@udf(returnType=BooleanType())
def is_weekend(date_str):
    """Check if date is weekend"""
    from datetime import datetime
    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return dt.weekday() >= 5  # 5=Saturday, 6=Sunday
```

#### 🧪 Test độc lập:
```bash
# Test UDFs
python3 test_local_features.py --test udf

# Expected:
# - All 4 UDFs work correctly
# - Pandas UDF 10-100x faster than Regular UDF
# - Output: review_df with new columns (rating_category, sentiment_score, etc.)
```

---

## 4. CÁCH TEST LOCAL

### 🎯 Test Strategy:

**Level 1: Unit Test** (Test từng feature riêng)
```bash
cd /home/user/bigdata-2025-1/Spark_Batch

# Test Window Functions only
python3 test_local_features.py --test window --data ../data/

# Test Broadcast Join only
python3 test_local_features.py --test broadcast --data ../data/

# Test Pivot only
python3 test_local_features.py --test pivot --data ../data/

# Test UDFs only
python3 test_local_features.py --test udf --data ../data/
```

**Level 2: Integration Test** (Test tích hợp)
```bash
# Test all new features
python3 test_local_features.py --test all --data ../data/

# Compare old vs new (7 analyses)
python3 test_local_features.py --test compare --data ../data/
```

**Level 3: Full Pipeline Test**
```bash
# Run full pipeline với 9 analyses (7 old + 2 new)
python3 batch_main_v2.py --data-path ../data/
```

---

### 🧪 Test Script Structure:

File `test_local_features.py` sẽ có:

```python
#!/usr/bin/env python3
"""
Test script for Spark advanced features (Phương án 1)
"""
import argparse
from pyspark.sql import SparkSession

def test_window_functions(spark, data_path):
    """Test Analysis 8: Window Functions"""
    print("Testing Window Functions...")
    # Load data
    # Run Analysis 8
    # Verify results
    # Print summary
    pass

def test_broadcast_join(spark, data_path):
    """Test Broadcast Join optimization"""
    print("Testing Broadcast Join...")
    # Run old Analysis 1 (without broadcast)
    # Run new Analysis 1 (with broadcast)
    # Compare results (should be identical)
    # Compare performance (new should be faster)
    pass

def test_pivot_unpivot(spark, data_path):
    """Test Analysis 9: Pivot/Unpivot"""
    print("Testing Pivot/Unpivot...")
    # Run Analysis 9
    # Verify pivot result
    # Verify unpivot result
    pass

def test_udfs(spark, data_path):
    """Test UDF library"""
    print("Testing UDFs...")
    # Load UDFs
    # Apply each UDF
    # Verify outputs
    # Compare performance (Pandas vs Regular)
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', choices=['window', 'broadcast', 'pivot', 'udf', 'all', 'compare'])
    parser.add_argument('--data', default='../data/')
    args = parser.parse_args()

    # Create Spark session
    spark = SparkSession.builder.appName("Feature Test").getOrCreate()

    # Run tests
    if args.test == 'window':
        test_window_functions(spark, args.data)
    elif args.test == 'broadcast':
        test_broadcast_join(spark, args.data)
    # ... etc
```

---

### 📊 Expected Output:

**Test Window Functions:**
```
Testing Window Functions...
✓ Loaded 100 businesses, 1000 reviews
✓ Analysis 8 completed in 3.45s
✓ Found 10 trending businesses

Top 3 Trending:
+------------+------------------+-------------+------------+----------------+------+
|business_id |name              |weekly_count |growth_rate |avg_last_4_weeks|rank  |
+------------+------------------+-------------+------------+----------------+------+
|business_001|Restaurant ABC    |45           |0.875       |32.5            |1     |
|business_023|Coffee Shop XYZ   |38           |0.722       |28.3            |2     |
|business_012|Bar & Grill       |32           |0.600       |24.0            |3     |
+------------+------------------+-------------+------------+----------------+------+

✅ Window Functions test PASSED
```

**Test Broadcast Join:**
```
Testing Broadcast Join...
✓ Running old Analysis 1 (no broadcast)... 5.23s
✓ Running new Analysis 1 (with broadcast)... 3.87s

Performance improvement: 26% faster
Results match: ✅ 100% identical

Spark Physical Plan (new):
- BroadcastHashJoin confirmed ✅

✅ Broadcast Join test PASSED
```

**Test Pivot/Unpivot:**
```
Testing Pivot/Unpivot...
✓ Analysis 9 completed

Pivot result (sample):
+------------+---------+----------+-----------+
|category    |Phoenix  |Las Vegas |Toronto    |
+------------+---------+----------+-----------+
|Restaurants |4.5 (120)|4.3 (95)  |4.2 (88)   |
|Shopping    |4.1 (45) |4.0 (38)  |3.9 (42)   |
+------------+---------+----------+-----------+

Unpivot result:
+------------+-----------+-----------+-------------+
|category    |city       |avg_stars  |review_count |
+------------+-----------+-----------+-------------+
|Restaurants |Phoenix    |4.5        |120          |
|Restaurants |Las Vegas  |4.3        |95           |
...

✅ Pivot/Unpivot test PASSED
```

**Test UDFs:**
```
Testing UDFs...

1. categorize_rating (Regular UDF):
   Time: 2.34s
   Sample: 4.5 → "Excellent", 3.2 → "Good"
   ✅ PASS

2. sentiment_score (Pandas UDF):
   Time: 0.21s (11x faster!)
   Sample: "Great food!" → 0.85, "Terrible service" → 0.15
   ✅ PASS

3. extract_keywords (Pandas UDF):
   Time: 0.45s
   Sample: "Amazing food and great service" → "food, great, service, amazing"
   ✅ PASS

4. is_weekend (Regular UDF):
   Time: 1.23s
   Sample: "2023-01-14" → True (Saturday)
   ✅ PASS

Performance comparison:
- Pandas UDF ~10-50x faster than Regular UDF ✅

✅ UDF test PASSED
```

---

### 📈 Verification Checklist:

**Sau mỗi test, verify:**
- [ ] Code chạy không lỗi
- [ ] Output đúng format expected
- [ ] Performance acceptable (< 10s với sample data)
- [ ] Kết quả có ý nghĩa business (không có giá trị vô lý)
- [ ] Spark UI confirm optimization (broadcast, etc.)

---

## 5. TROUBLESHOOTING

### ❌ Lỗi thường gặp:

**1. Import error: pandas/pyarrow**
```bash
# Fix:
pip install pandas==2.0.3 pyarrow==12.0.0
```

**2. Window function error: "column not found"**
```python
# Fix: Ensure correct column names
windowSpec = Window.partitionBy("business_id").orderBy("week_start")
# Check: df.columns để xem tên cột chính xác
```

**3. Broadcast join not applied**
```python
# Fix: Check Spark UI → SQL tab → Physical Plan
# Should see: "BroadcastHashJoin" not "SortMergeJoin"

# Debug:
df.explain()  # Check execution plan
```

**4. UDF slow performance**
```python
# Fix: Use Pandas UDF instead of Regular UDF
# Regular UDF: Row-by-row (slow)
# Pandas UDF: Vectorized (fast)
```

**5. Out of memory**
```bash
# Fix: Reduce data size for testing
head -1000 data/review.json > data/review_sample.json

# Or increase Spark memory:
# Edit batch_configuration.py:
.config("spark.driver.memory", "8g")
```

---

## 6. EXPECTED TIMELINE

### Day 1: UDF + Window Functions
```
Morning (3-4h):
- [x] Create batch_udf.py
- [x] Test UDFs independently
- [x] Fix any issues

Afternoon (3-4h):
- [x] Create batch_analytics_advanced.py
- [x] Implement Analysis 8 (Window Functions)
- [x] Test Analysis 8
- [x] Verify trending results
```

### Day 2: Pivot + Broadcast Join
```
Morning (2-3h):
- [x] Implement Analysis 9 (Pivot/Unpivot)
- [x] Test Analysis 9
- [x] Verify matrix results

Afternoon (3-4h):
- [x] Refactor 7 analyses với Broadcast Join
- [x] Test performance comparison
- [x] Verify correctness
```

### Day 3: Integration + Documentation
```
Morning (2-3h):
- [x] Create batch_main_v2.py
- [x] Integration test all 9 analyses
- [x] Fix any integration issues

Afternoon (2-3h):
- [x] Create test_local_features.py
- [x] Run full test suite
- [x] Update documentation
- [x] Commit & push
```

---

## 7. NEXT STEPS

### ✅ Ready to start?

**Tôi sẽ tạo các files sau theo thứ tự:**

1. `batch_udf.py` - UDF library
2. `batch_analytics_advanced.py` - Analysis 8, 9
3. `test_local_features.py` - Test script
4. Refactor `batch_analytics.py` - Add broadcast joins
5. `batch_main_v2.py` - Integration
6. `LOCAL_TEST_GUIDE.md` - Step-by-step test guide

**Sau đó bạn sẽ:**
1. Chạy test từng phần
2. Verify kết quả
3. Chạy full pipeline
4. Review và approve

---

**Bạn đã sẵn sàng? Tôi sẽ bắt đầu tạo code! 🚀**

**Hoặc bạn có câu hỏi nào về plan này không?**
