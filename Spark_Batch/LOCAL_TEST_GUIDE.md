# 🧪 LOCAL TEST GUIDE - Phương án 1 (Advanced Spark Features)

**Mục đích:** Hướng dẫn test từng bước trên local để verify 4 kỹ năng Spark nâng cao

**Score mục tiêu:** 42% → 64%

---

## 📋 MỤC LỤC

1. [Chuẩn bị môi trường](#1-chuẩn-bị-môi-trường)
2. [Test từng feature](#2-test-từng-feature)
3. [Test tích hợp](#3-test-tích-hợp)
4. [Verify kết quả](#4-verify-kết-quả)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. CHUẨN BỊ MÔI TRƯỜNG

### Bước 1.1: Kiểm tra dependencies

```bash
cd /home/user/bigdata-2025-1/Spark_Batch

# Check Python
python3 --version
# Expected: Python 3.8+

# Check Java
java -version
# Expected: Java 11+

# Check PySpark
python3 -c "import pyspark; print(pyspark.__version__)"
# Expected: 4.0.1
```

### Bước 1.2: Cài thêm dependencies cho Pandas UDF

```bash
# Cài pandas và pyarrow (cần cho Pandas UDF)
pip3 install pandas==2.0.3 pyarrow==12.0.0

# Verify
python3 -c "import pandas, pyarrow; print('OK')"
```

### Bước 1.3: Chuẩn bị data

**Option A: Tạo sample data**
```bash
# Tạo sample data nhanh (100 businesses, 1000 reviews)
python3 create_sample_data.py
```

**Option B: Sử dụng data có sẵn**
```bash
# Đảm bảo data files tồn tại
ls -lh ../data/business.json
ls -lh ../data/review.json
```

---

## 2. TEST TỪNG FEATURE

### Test 2.1: UDF Library ⭐⭐⭐⭐

**Mục đích:** Test 7 UDFs (3 Regular + 4 Pandas)

**Chạy test:**
```bash
# Test UDF demo trước
python3 batch_udf.py
```

**Expected output:**
```
==============================================================
UDF LIBRARY - DEMO & TEST
==============================================================

1. Creating sample data and applying UDFs...

Results:
+----------+-----+----------------+---------+----------+
|review_id |stars|rating_category |sentiment|is_weekend|
+----------+-----+----------------+---------+----------+
|review_0  |3.5  |Good            |0.85     |false     |
|review_1  |4.0  |Good            |0.15     |true      |
...

✓ All UDFs working correctly!
```

**Verify:**
- [ ] 4 UDFs chạy không lỗi
- [ ] Sentiment score hợp lý (0.0-1.0)
- [ ] Keywords được extract ra
- [ ] Weekend detection chính xác

**Chạy full test:**
```bash
python3 test_local_features.py --test udf --data ../data/
```

**Expected output:**
```
==============================================================
TEST 1: UDF LIBRARY
==============================================================

Testing Regular UDFs...
  1. categorize_rating...
     Time: 2.34s ✓
  2. is_weekend...
     ✓

Testing Pandas UDFs (vectorized)...
  3. sentiment_score...
     Time: 0.21s ✓
  4. extract_keywords...
     ✓

==============================================================
Performance Comparison:
  Regular UDF:  2.34s
  Pandas UDF:   0.21s
  Speedup:      11.1x faster! ✅
==============================================================

✅ UDF Test PASSED
```

**Verify:**
- [ ] Pandas UDF nhanh hơn Regular UDF (5-20x)
- [ ] Không có error
- [ ] Test PASSED

---

### Test 2.2: Window Functions ⭐⭐⭐⭐⭐

**Mục đích:** Test Analysis 8 (Trending Businesses)

**Chạy test:**
```bash
python3 test_local_features.py --test window --data ../data/
```

**Expected output:**
```
==============================================================
TEST 2: WINDOW FUNCTIONS (Analysis 8)
==============================================================

  - Analyzing reviews from 90 days
  - Grouped into weekly buckets
  - Applied window functions: lag, lead, avg, sum, row_number
✓ Analysis 8 completed in 3.45s
  Found 10 trending businesses

Trending Businesses (Top 5):
+------------------+----------+-------------+------------+----------------+-----------+
|name              |city      |weekly_count |growth_rate |avg_last_4_weeks|trend_rank |
+------------------+----------+-------------+------------+----------------+-----------+
|Restaurant ABC    |Phoenix   |45           |0.875       |32.5            |1          |
|Coffee Shop XYZ   |Las Vegas |38           |0.722       |28.3            |2          |
...

==============================================================
Window Functions Verified:
  ✓ lag() - Previous week comparison
  ✓ avg() over window - Moving average
  ✓ dense_rank() - Ranking
  ✓ sum() over window - Cumulative sum
  ✓ row_number() - Week numbering
==============================================================

✅ Window Functions Test PASSED
```

**Verify:**
- [ ] Có growth_rate (từ lag)
- [ ] Có avg_last_4_weeks (từ avg over window)
- [ ] Có trend_rank (từ dense_rank)
- [ ] Kết quả có nghĩa (growth_rate > 0)
- [ ] Test PASSED

---

### Test 2.3: Pivot/Unpivot ⭐⭐⭐⭐

**Mục đích:** Test Analysis 9 (Category Performance Matrix)

**Chạy test:**
```bash
python3 test_local_features.py --test pivot --data ../data/
```

**Expected output:**
```
==============================================================
TEST 3: PIVOT/UNPIVOT (Analysis 9)
==============================================================

  - Finding top 5 categories and 3 cities...
  - Top categories: Restaurants, Shopping, Food...
  - Top cities: Phoenix, Las Vegas, Toronto...
  - Aggregated metrics by category and city
  - Creating pivot table...
  - Pivot complete: 3 cities, 5 categories
  - Creating unpivot table...
  - Unpivot complete: 15 rows

Pivot Result (Wide format):
+------------+--------------+-------------+----------+
|category    |Phoenix       |Las Vegas    |Toronto   |
+------------+--------------+-------------+----------+
|Restaurants |4.5 (120)     |4.3 (95)     |4.2 (88)  |
|Shopping    |4.1 (45)      |4.0 (38)     |3.9 (42)  |
...

Unpivot Result (Long format - sample):
+------------+-----------+-----------+-------------+
|category    |city       |avg_stars  |review_count |
+------------+-----------+-----------+-------------+
|Restaurants |Phoenix    |4.5        |120          |
|Restaurants |Las Vegas  |4.3        |95           |
...

Best Category per City:
+-----------+--------------+-----------+-------------+
|city       |best_category |avg_stars  |review_count |
+-----------+--------------+-----------+-------------+
|Phoenix    |Restaurants   |4.5        |120          |
...

==============================================================
Pivot/Unpivot Operations Verified:
  ✓ explode() - Split categories
  ✓ pivot() - Transform long → wide
  ✓ stack() - Transform wide → long
==============================================================

✅ Pivot/Unpivot Test PASSED
```

**Verify:**
- [ ] Pivot: Wide table (categories x cities)
- [ ] Unpivot: Long table (category, city, metrics)
- [ ] Có best_per_city và best_per_category
- [ ] Test PASSED

---

### Test 2.4: Broadcast Join ⭐⭐⭐⭐

**Mục đích:** Test Broadcast Join optimization

**Chạy test:**
```bash
python3 test_local_features.py --test broadcast --data ../data/
```

**Expected output:**
```
==============================================================
TEST 4: BROADCAST JOIN OPTIMIZATION
==============================================================

Running Analysis 1 with Broadcast Join...

Analysis 1: Top 10 Selling Products (Last 90 days)
✓ Analysis 1 completed in 3.87s

Execution time: 3.87s

==============================================================
Checking Physical Plan...
✅ Broadcast Join confirmed in physical plan!
==============================================================

✅ Broadcast Join Test PASSED
```

**Verify:**
- [ ] Chạy không lỗi
- [ ] Physical plan có "BroadcastHashJoin"
- [ ] Test PASSED

---

## 3. TEST TÍCH HỢP

### Test 3.1: Test tất cả features

```bash
# Test all features cùng lúc
python3 test_local_features.py --test all --data ../data/
```

**Expected output:**
```
================================================================================
                  SPARK ADVANCED FEATURES - TEST SUITE
                            (Phương án 1)
================================================================================

==============================================================
LOADING TEST DATA
==============================================================
✓ Loaded 100 businesses
✓ Loaded 1,000 reviews

[... run all 4 tests ...]

================================================================================
                            TEST SUMMARY
================================================================================
  ✅ UDF: PASS
  ✅ WINDOW: PASS
  ✅ PIVOT: PASS
  ✅ BROADCAST: PASS
================================================================================
                      🎉 ALL TESTS PASSED! 🎉
================================================================================
```

**Verify:**
- [ ] Tất cả 4 tests PASSED
- [ ] Không có error
- [ ] Summary hiển thị OK

---

### Test 3.2: Run full pipeline (9 analyses)

```bash
# Chạy pipeline đầy đủ
python3 batch_main_v2.py --data-path ../data/
```

**Expected output:**
```
================================================================================
               YELP BIG DATA ANALYSIS SYSTEM - VERSION 2
                    BATCH MODE - Enhanced with Advanced Features
                        Run Time: 2025-12-15 15:30:00
================================================================================

NEW FEATURES:
  ✨ Window Functions (Analysis 8: Trending Businesses)
  ✨ Pivot/Unpivot Operations (Analysis 9: Performance Matrix)
  ✨ Broadcast Join Optimization (Analyses 1-7)
  ✨ UDF Library Support (7 custom functions)
================================================================================

==============================================================
DATA LOADING PHASE
==============================================================
✓ Loaded 100 businesses from ../data/business.json
✓ Loaded 1,000 reviews from ../data/review.json

✓ All data loaded successfully

==============================================================
ANALYSIS PHASE - RUNNING ALL 9 ANALYSES
==============================================================

==============================================================
PART 1: Original Analyses (1-7)
==============================================================

Analysis 1: Top 10 Selling Products (Last 90 days)
✓ Analysis 1 completed in 2.34s

Analysis 2: Top 10 Stores by Product Diversity
✓ Analysis 2 completed in 1.23s

[... Analyses 3-7 ...]

==============================================================
PART 2: Advanced Analyses (8-9)
==============================================================

Analysis 8: Top 10 Trending Businesses (Last 90 days)
✓ Analysis 8 completed in 3.45s

Analysis 9: Category Performance Matrix (Pivot/Unpivot)
✓ Analysis 9 completed in 2.87s

==============================================================
ALL 9 ANALYSES COMPLETED in 25.34s
==============================================================

[... Results preview for 9 analyses ...]

================================================================================
                            SUMMARY REPORT - V2
================================================================================

==============================================================
Part 1: Original Analyses (1-7)
==============================================================
  TOP SELLING: 10 records
  DIVERSE STORES: 10 records
  BEST RATED: 10 records
  MOST POSITIVE: 10 records
  PEAK HOURS: 24 records
  TOP CATEGORIES: 20 records
  STORE STATS: 100 records

==============================================================
Part 2: Advanced Analyses (8-9)
==============================================================
  TRENDING BUSINESSES: 10 businesses
  CATEGORY MATRIX: 15 category-city pairs
  CATEGORIES ANALYZED: 5
  CITIES ANALYZED: 3
  TOTAL REVIEWS: 1,000
==============================================================

✓ All results saved to: ./output_v2/

================================================================================
                    ✓ PIPELINE COMPLETED SUCCESSFULLY
                          9 Analyses Executed
================================================================================
```

**Verify:**
- [ ] 9 analyses chạy thành công
- [ ] Không có error
- [ ] Có output files trong `./output_v2/`
- [ ] Pipeline COMPLETED

---

## 4. VERIFY KẾT QUẢ

### Verify 4.1: Kiểm tra output files

```bash
# Liệt kê output files
ls -lh output_v2/

# Expected:
# top_selling/
# diverse_stores/
# best_rated/
# most_positive/
# peak_hours/
# top_categories/
# store_stats/
# trending_businesses/          ← NEW
# category_matrix_pivot/        ← NEW
# category_matrix_unpivot/      ← NEW
```

### Verify 4.2: Đọc và verify kết quả

```bash
# Test đọc Parquet output
python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Verify").getOrCreate()

# Đọc Analysis 8 output
trending = spark.read.parquet("output_v2/trending_businesses/")
print(f"Trending businesses: {trending.count()} rows")
trending.show(5, truncate=False)

# Verify có columns từ window functions
assert "growth_rate" in trending.columns, "Missing growth_rate column!"
assert "avg_last_4_weeks" in trending.columns, "Missing avg_last_4_weeks column!"
print("✅ Analysis 8 output verified!")

# Đọc Analysis 9 output
pivot = spark.read.parquet("output_v2/category_matrix_pivot/")
print(f"\nCategory matrix: {pivot.count()} rows")
pivot.show(truncate=False)
print("✅ Analysis 9 output verified!")

spark.stop()
EOF
```

### Verify 4.3: Performance metrics

**So sánh với version cũ:**

| Metric | V1 (Old) | V2 (New) | Improvement |
|--------|----------|----------|-------------|
| Total analyses | 7 | 9 | +2 analyses |
| Broadcast joins | 0 explicit | 4 explicit | ✅ Optimized |
| Window functions | ❌ | ✅ | ✅ Added |
| Pivot/Unpivot | ❌ | ✅ | ✅ Added |
| UDF library | ❌ | ✅ 7 UDFs | ✅ Added |
| Skill score | 42% | 64% | +22% |

---

## 5. TROUBLESHOOTING

### Issue 5.1: Import error - pandas/pyarrow

**Error:**
```
ModuleNotFoundError: No module named 'pandas'
```

**Fix:**
```bash
pip3 install pandas==2.0.3 pyarrow==12.0.0
```

---

### Issue 5.2: Window function error

**Error:**
```
AnalysisException: window() is not supported on streaming DataFrames
```

**Fix:**
Code đang chạy batch mode, không phải streaming. Check xem có import đúng `batch_analytics_advanced` không.

---

### Issue 5.3: Broadcast join not working

**Error:**
Physical plan không có "BroadcastHashJoin"

**Debug:**
```python
# Check physical plan
result.explain()

# Should see: "BroadcastHashJoin" or "BroadcastNestedLoopJoin"
```

**Possible causes:**
- Business table quá lớn (> 10MB) → Spark tự động không broadcast
- Chưa import `broadcast` function
- Config `spark.sql.autoBroadcastJoinThreshold` quá nhỏ

---

### Issue 5.4: Out of memory

**Error:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Fix:**
```python
# Edit batch_configuration.py
.config("spark.driver.memory", "4g")  # Giảm từ 8g

# Hoặc reduce data size
head -100 ../data/review.json > ../data/review_sample.json
```

---

### Issue 5.5: Sample data không đủ

**Error:**
```
Analysis 8 completed in 3.45s
  Found 0 trending businesses
```

**Fix:**
Sample data có thể không có đủ data trong 90 ngày gần đây.

```bash
# Option 1: Giảm window_days
python3 batch_main_v2.py --data-path ../data/
# Edit config: window_days=30 (thay vì 90)

# Option 2: Tạo sample data lớn hơn
python3 << 'EOF'
from create_sample_data import *
businesses = generate_business_data(num_businesses=500)
reviews = generate_review_data(num_reviews=5000, num_businesses=500)
save_json_lines(businesses, "../data/business.json")
save_json_lines(reviews, "../data/review.json")
EOF
```

---

## 6. CHECKLIST HOÀN THÀNH

### Phase 1: Setup ✅
- [ ] Python 3.8+ installed
- [ ] Java 11+ installed
- [ ] PySpark 4.0.1 installed
- [ ] Pandas + PyArrow installed
- [ ] Data files ready

### Phase 2: Individual Tests ✅
- [ ] UDF test PASSED
- [ ] Window Functions test PASSED
- [ ] Pivot/Unpivot test PASSED
- [ ] Broadcast Join test PASSED

### Phase 3: Integration ✅
- [ ] test_local_features.py --test all PASSED
- [ ] batch_main_v2.py runs successfully
- [ ] 9 analyses completed
- [ ] Output files created

### Phase 4: Verification ✅
- [ ] Output files readable
- [ ] Results make sense (no NaN, no empty)
- [ ] Performance acceptable (< 60s for sample data)
- [ ] Spark UI confirms optimizations

---

## 7. NEXT STEPS

**Sau khi tất cả tests PASS:**

1. ✅ **Commit code**
2. ✅ **Update documentation**
3. ✅ **Prepare demo**
4. 🎉 **Score: 42% → 64% achieved!**

**Nếu muốn lên 83% (Phương án 2):**
- Add Machine Learning Pipeline
- Add Graph Processing
- Add Advanced Statistics

---

**Congratulations! 🎉**

Bạn đã hoàn thành Phương án 1 với 4 kỹ năng Spark nâng cao:
- ✅ Window Functions
- ✅ Broadcast Join
- ✅ Pivot/Unpivot
- ✅ UDF/Pandas UDF

**Skill score: 42% → 64%** ⭐⭐⭐⭐
