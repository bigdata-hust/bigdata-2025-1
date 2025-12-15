# 📊 ĐÁNH GIÁ KỸ NĂNG SPARK - DỰ ÁN YELP ANALYSIS

**Mục đích:** Đánh giá dự án theo yêu cầu kỹ năng Spark mức trung cấp và đề xuất phương án bổ sung.

**Ngày đánh giá:** 2025-12-15

---

## 📋 MỤC LỤC

1. [Tổng quan](#1-tổng-quan)
2. [Chi tiết đánh giá](#2-chi-tiết-đánh-giá)
3. [Kỹ năng còn thiếu](#3-kỹ-năng-còn-thiếu)
4. [Phương án bổ sung](#4-phương-án-bổ-sung)
5. [Roadmap triển khai](#5-roadmap-triển-khai)

---

## 1. TỔNG QUAN

### ✅ Điểm mạnh hiện tại:
- Có 2 phiên bản: Streaming (nhánh HAI) và Batch (Spark_Batch)
- 7 hàm phân tích hoàn chỉnh
- Code structure tốt, modular
- Có documentation đầy đủ

### ⚠️ Điểm yếu:
- Thiếu nhiều kỹ thuật Spark nâng cao
- Chưa có Window Functions, Pivot/Unpivot
- Chưa có UDF tùy chỉnh
- Chưa có Broadcast Join optimization rõ ràng
- Chưa có Machine Learning

---

## 2. CHI TIẾT ĐÁNH GIÁ

### 📊 2.1. TẬP HỢP PHỨC TẠP (Complex Aggregations)

#### ✅ ĐÃ CÓ:

**Hàm tổng hợp cơ bản:**
```python
# File: batch_analytics.py, dòng 42-45
business_stats = salted_agg.groupBy("business_id").agg(
    sum("partial_count").alias("recent_review_count"),
    (sum("partial_sum_stars") / sum("partial_count_stars")).alias("avg_rating")
)
```

**Tổng hợp với điều kiện (Conditional Aggregation):**
```python
# File: batch_analytics.py, dòng 172-187
review_stats = review_df.groupBy("business_id").agg(
    sum(when(col("stars") >= positive_threshold, 1).otherwise(0))
        .alias("positive_review_count"),
    count("review_id").alias("total_review_count"),
    avg(when(col("stars") >= positive_threshold, col("stars")))
        .alias("avg_positive_rating"),
    sum(when(col("stars") >= positive_threshold, col("useful")).otherwise(0))
        .alias("total_useful_votes")
)
```

**Multi-stage aggregation (Salted):**
```python
# File: batch_analytics.py, dòng 35-45
# Stage 1: Salted aggregation to avoid skew
salted_agg = recent_reviews.groupBy("business_id", "salt").agg(...)
# Stage 2: Final aggregation
business_stats = salted_agg.groupBy("business_id").agg(...)
```

#### ❌ CHƯA CÓ:

1. **Window Functions** - THIẾU HOÀN TOÀN
   - `row_number()`, `rank()`, `dense_rank()`
   - `lag()`, `lead()` cho time series
   - `cumsum()`, `cummean()` cho cumulative metrics
   - Partitioning và ordering trong window

2. **Pivot/Unpivot Operations** - THIẾU
   - Chuyển đổi dữ liệu từ long → wide format
   - Phân tích cross-tabulation
   - Dynamic pivoting

3. **Custom Aggregate Functions** - THIẾU
   - UDAF (User Defined Aggregate Functions)
   - Tổng hợp phức tạp với logic tùy chỉnh

**Mức độ đáp ứng:** 40% ⭐⭐

---

### 🔄 2.2. BIẾN ĐỔI NÂNG CAO (Advanced Transformations)

#### ✅ ĐÃ CÓ:

**Multi-stage transformations:**
```python
# File: batch_analytics.py, dòng 24-45
review_with_salt = review_df.withColumn("salt", (rand() * 10).cast("int"))
review_with_salt = review_with_salt.withColumn('date_parsed', to_timestamp(...))
recent_reviews = review_with_salt.filter(col("date_parsed") >= cutoff_date)
salted_agg = recent_reviews.groupBy(...).agg(...)
business_stats = salted_agg.groupBy(...).agg(...)
```

**Complex operations:**
```python
# File: batch_analytics.py, dòng 90-92
result = business_filtered.withColumn(
    "category_count",
    size(split(trim(col("categories")), "\\s*,\\s*"))
)
```

#### ❌ CHƯA CÓ:

1. **UDF (User Defined Functions)** - THIẾU
   - Pandas UDF (vectorized)
   - Custom business logic functions
   - Text processing UDF

2. **Complex chaining** - CÓ NHƯNG ÍT
   - Hiện tại chỉ có 2-3 stages
   - Chưa có pipeline phức tạp > 5 stages

3. **Data quality transformations** - THIẾU
   - Outlier detection và handling
   - Missing value imputation
   - Data validation logic

**Mức độ đáp ứng:** 50% ⭐⭐⭐

---

### 🔗 2.3. JOIN OPERATIONS

#### ✅ ĐÃ CÓ:

**Basic joins:**
```python
# File: batch_analytics.py, dòng 51-53
result = top_candidates.join(
    business_df.select("business_id", "name", "city", "state", "categories"),
    "business_id"
)
```

**Join optimization (limit before join):**
```python
# File: batch_analytics.py, dòng 48
top_candidates = business_stats.orderBy(desc("recent_review_count")).limit(top_n * 10)
# Sau đó mới join
```

#### ❌ CHƯA CÓ:

1. **Broadcast Join** - CHƯA RÕ RÀNG
   - Code comment có đề cập "Broadcast join" nhưng không thấy `broadcast()` function
   - Cần explicit `from pyspark.sql.functions import broadcast`

2. **Sort-Merge Join optimization** - THIẾU
   - Chưa có repartition by join key
   - Chưa có bucketing strategy

3. **Multiple join optimization** - THIẾU
   - Chưa có phân tích và tối ưu multiple joins
   - Chưa có join reordering strategy

4. **Skewed join handling** - CÓ NHƯNG ÍT
   - Có salting cho aggregation
   - Chưa có salting cho skewed joins

**Mức độ đáp ứng:** 40% ⭐⭐

---

### ⚡ 2.4. TỐI ƯU HÓA HIỆU NĂNG

#### ✅ ĐÃ CÓ:

**Caching:**
```python
# File: batch_load_data.py, dòng 43, 66
business_df.cache()
review_df.cache()
```

**Data skew handling (Salting):**
```python
# File: batch_analytics.py, dòng 25
review_with_salt = review_df.withColumn("salt", (rand() * 10).cast("int"))
```

**Early filtering:**
```python
# File: batch_analytics.py, dòng 122
business_stats = review_df.filter(col("stars").isNotNull()).groupBy(...)
```

**Column pruning:**
```python
# File: batch_analytics.py, dòng 52
business_df.select("business_id", "name", "city", "state", "categories")
```

#### ❌ CHƯA CÓ:

1. **Partitioning strategy** - THIẾU CHI TIẾT
   - Config có `spark.sql.shuffle.partitions` nhưng chưa dynamic
   - Chưa có repartition() theo business logic
   - Chưa có coalesce() khi cần

2. **Persistence strategy** - CƠ BẢN
   - Chỉ dùng `cache()`, chưa có `persist(StorageLevel.xxx)`
   - Chưa có memory vs disk tradeoff

3. **Query optimization** - THIẾU
   - Chưa có `explain()` analysis
   - Chưa có cost-based optimization tuning
   - Chưa có adaptive query execution monitoring

4. **Pruning** - CƠ BẢN
   - Có column pruning
   - Chưa có partition pruning (vì không có partitioned data)

**Mức độ đáp ứng:** 50% ⭐⭐⭐

---

### 🌊 2.5. XỬ LÝ STREAMING

#### ✅ ĐÃ CÓ (Nhánh HAI):

**Structured Streaming:**
```python
# File: Spark/load_data.py (nhánh HAI)
business_df = (
    self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", self.kafka_broker)
        .option("subscribe", "business")
        .option("startingOffsets", "earliest")
        .load()
)
```

**Watermarking:**
```python
# File: Spark/load_data.py (nhánh HAI), dòng 37
.withWatermark('business_ts', '10 minutes')
```

**Multiple output modes:**
```python
# File: Spark/pipeline_orchestration.py (nhánh HAI)
# - append mode cho HDFS
# - foreachBatch cho Elasticsearch
```

**Checkpointing:**
```python
# File: Spark/configuration.py (nhánh HAI), dòng 70
spark.sparkContext.setCheckpointDir(os.path.abspath("checkpoints"))
```

#### ❌ CHƯA CÓ:

1. **State management** - CƠ BẢN
   - Có watermarking nhưng chưa rõ state store strategy
   - Chưa có stateful operations với `mapGroupsWithState`

2. **Exactly-once processing** - CHƯA RÕ
   - Chưa có idempotent writes
   - Chưa có transaction handling

3. **Late data handling** - CƠ BẢN
   - Có watermark 10 minutes
   - Chưa có complex late data policy

4. **Multiple streaming queries coordination** - THIẾU
   - Chưa có query monitoring và coordination

**Mức độ đáp ứng (Nhánh HAI):** 60% ⭐⭐⭐
**Mức độ đáp ứng (Spark_Batch):** 0% (không có streaming)

---

### 🤖 2.6. PHÂN TÍCH NÂNG CAO

#### ✅ ĐÃ CÓ:

**Statistical computation (basic):**
```python
# avg, sum, count, stddev (có sử dụng)
avg("stars"), count("review_id"), sum("useful")
```

**Time series analysis (basic):**
```python
# File: batch_analytics.py, dòng 244-251
result = df.groupBy(
    year("date_parsed").alias("year"),
    month("date_parsed").alias("month")
).agg(count("review_id").alias("review_count"))
```

#### ❌ CHƯA CÓ:

1. **Machine Learning với MLlib** - THIẾU HOÀN TOÀN
   - Nhánh HAI có model TF-IDF sentiment nhưng không thấy code training
   - Chưa có feature engineering
   - Chưa có model evaluation
   - Chưa có pipeline ML

2. **Graph processing với GraphFrames** - THIẾU
   - Chưa có social network analysis
   - Chưa có user-business graph
   - Chưa có PageRank, Community Detection

3. **Advanced statistics** - THIẾU
   - Chưa có correlation analysis
   - Chưa có hypothesis testing
   - Chưa có anomaly detection

4. **Time series advanced** - THIẾU
   - Chưa có trend analysis
   - Chưa có seasonality detection
   - Chưa có forecasting

**Mức độ đáp ứng:** 10% ⭐

---

## 3. KỸ NĂNG CÒN THIẾU

### 🔴 Ưu tiên CAO (Critical):

| # | Kỹ năng thiếu | Lý do quan trọng | Độ khó |
|---|--------------|------------------|--------|
| 1 | **Window Functions** | Cần thiết cho ranking, moving averages | Trung bình |
| 2 | **Broadcast Join (explicit)** | Tối ưu joins với small tables | Dễ |
| 3 | **UDF/Pandas UDF** | Custom business logic | Trung bình |
| 4 | **Machine Learning Pipeline** | Yêu cầu cốt lõi cho advanced analytics | Khó |

### 🟡 Ưu tiên TRUNG BÌNH (Important):

| # | Kỹ năng thiếu | Lý do quan trọng | Độ khó |
|---|--------------|------------------|--------|
| 5 | **Pivot/Unpivot** | Data transformation cho reporting | Dễ |
| 6 | **Custom UDAF** | Tổng hợp phức tạp | Khó |
| 7 | **Advanced partitioning** | Performance optimization | Trung bình |
| 8 | **Graph processing** | Social network insights | Khó |

### 🟢 Ưu tiên THẤP (Nice to have):

| # | Kỹ năng thiếu | Lý do | Độ khó |
|---|--------------|-------|--------|
| 9 | **Advanced time series** | Forecasting | Khó |
| 10 | **Streaming state management** | Advanced streaming | Khó |

---

## 4. PHƯƠNG ÁN BỔ SUNG

### 🎯 Phương án 1: BỔ SUNG NHANH (2-3 ngày)

**Mục tiêu:** Bổ sung các kỹ năng dễ và có tác động lớn

#### 4.1.1. Thêm Window Functions (1 ngày)

**Analysis mới: Analysis 8 - Trending Businesses**

```python
@staticmethod
def trending_businesses(business_df, review_df, window_days=30):
    """
    8. Trending Businesses Analysis
    Find businesses with increasing review trends using window functions
    """
    from pyspark.sql.window import Window

    # Parse review dates
    df = review_df.withColumn(
        "review_date",
        to_date(col("date"), "yyyy-MM-dd HH:mm:ss")
    )

    # Group by business and week
    weekly_reviews = df.groupBy(
        "business_id",
        window("review_date", "7 days").alias("week")
    ).agg(
        count("review_id").alias("weekly_count")
    ).select(
        "business_id",
        col("week.start").alias("week_start"),
        "weekly_count"
    )

    # Define window: partition by business, order by week
    windowSpec = Window.partitionBy("business_id").orderBy("week_start")

    # Calculate trend metrics
    trending = weekly_reviews.withColumn(
        "prev_week_count", lag("weekly_count", 1).over(windowSpec)
    ).withColumn(
        "growth_rate",
        when(col("prev_week_count") > 0,
             (col("weekly_count") - col("prev_week_count")) / col("prev_week_count")
        ).otherwise(0)
    ).withColumn(
        "avg_last_4_weeks",
        avg("weekly_count").over(
            windowSpec.rowsBetween(-3, 0)
        )
    ).withColumn(
        "rank_this_week",
        dense_rank().over(
            Window.orderBy(desc("weekly_count"))
        )
    )

    # Filter recent and high-growth businesses
    result = trending.filter(
        col("week_start") >= date_sub(current_date(), window_days)
    ).filter(
        col("growth_rate") > 0.2  # 20% growth
    ).join(
        business_df.select("business_id", "name", "city", "categories"),
        "business_id"
    ).select(
        "business_id",
        "name",
        "city",
        "week_start",
        "weekly_count",
        "growth_rate",
        "avg_last_4_weeks",
        "rank_this_week"
    ).orderBy(desc("growth_rate"))

    return result
```

**Kỹ năng thể hiện:**
- ✅ `lag()`, `lead()` - window functions
- ✅ `avg()` over window - moving average
- ✅ `dense_rank()` - ranking
- ✅ `partitionBy` + `orderBy` - window specification
- ✅ `rowsBetween()` - frame specification

---

#### 4.1.2. Thêm Broadcast Join Explicit (0.5 ngày)

**Sửa lại các hàm hiện tại:**

```python
from pyspark.sql.functions import broadcast

# BEFORE (implicit broadcast)
result = top_candidates.join(
    business_df.select("business_id", "name", "city"),
    "business_id"
)

# AFTER (explicit broadcast)
result = top_candidates.join(
    broadcast(
        business_df.select("business_id", "name", "city")
    ),
    "business_id"
)
```

**Thêm analysis mới với skewed join:**

```python
@staticmethod
def handle_skewed_join_example(review_df, business_df):
    """
    Example: Handle skewed join with salting
    Some businesses have millions of reviews (skewed)
    """
    # Add salt to skewed side
    review_salted = review_df.withColumn(
        "salt", (rand() * 10).cast("int")
    ).withColumn(
        "business_id_salted",
        concat(col("business_id"), lit("_"), col("salt"))
    )

    # Replicate small side
    from pyspark.sql.functions import explode, array
    business_replicated = business_df.withColumn(
        "salt", explode(array([lit(i) for i in range(10)]))
    ).withColumn(
        "business_id_salted",
        concat(col("business_id"), lit("_"), col("salt"))
    )

    # Join on salted key
    result = review_salted.join(
        business_replicated,
        "business_id_salted"
    )

    return result
```

---

#### 4.1.3. Thêm Pivot/Unpivot (0.5 ngày)

**Analysis mới: Analysis 9 - Category Performance Matrix**

```python
@staticmethod
def category_performance_matrix(business_df, review_df):
    """
    9. Category Performance Matrix
    Pivot analysis: categories vs cities
    """
    # Explode categories
    df = business_df.withColumn(
        "category",
        explode(split(col("categories"), ",\\s*"))
    )

    # Join with reviews
    joined = df.join(review_df, "business_id")

    # Aggregate by category and city
    agg_df = joined.groupBy("category", "city").agg(
        avg("stars").alias("avg_stars"),
        count("review_id").alias("review_count")
    )

    # Pivot: categories as rows, cities as columns
    pivoted = agg_df.groupBy("category").pivot("city").agg(
        first("avg_stars").alias("avg_stars"),
        first("review_count").alias("count")
    )

    return pivoted
```

**Unpivot example:**

```python
@staticmethod
def unpivot_example(pivoted_df):
    """
    Unpivot: wide format → long format
    """
    from pyspark.sql.functions import expr, stack

    # Get city columns (all except category)
    city_cols = [c for c in pivoted_df.columns if c != 'category']

    # Stack columns
    unpivoted = pivoted_df.select(
        "category",
        expr(f"stack({len(city_cols)}, " +
             ", ".join([f"'{c}', `{c}`" for c in city_cols]) +
             ") as (city, avg_stars)")
    )

    return unpivoted
```

---

#### 4.1.4. Thêm UDF (1 ngày)

**Regular UDF:**

```python
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def categorize_rating(stars):
    """Custom UDF: Categorize rating"""
    if stars >= 4.5:
        return "Excellent"
    elif stars >= 3.5:
        return "Good"
    elif stars >= 2.5:
        return "Average"
    else:
        return "Poor"

# Usage
df = review_df.withColumn(
    "rating_category",
    categorize_rating(col("stars"))
)
```

**Pandas UDF (Vectorized):**

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(FloatType())
def sentiment_score(text: pd.Series) -> pd.Series:
    """
    Pandas UDF: Calculate sentiment score
    (Much faster than regular UDF)
    """
    # Simple sentiment: count positive/negative words
    positive_words = ['great', 'excellent', 'amazing', 'love', 'best']
    negative_words = ['bad', 'terrible', 'worst', 'hate', 'awful']

    def score(t):
        if pd.isna(t):
            return 0.0
        t_lower = t.lower()
        pos_count = sum(t_lower.count(w) for w in positive_words)
        neg_count = sum(t_lower.count(w) for w in negative_words)
        total = pos_count + neg_count
        if total == 0:
            return 0.5
        return pos_count / total

    return text.apply(score)

# Usage
df = review_df.withColumn(
    "sentiment_score",
    sentiment_score(col("text"))
)
```

**Tổng thời gian Phương án 1: 2-3 ngày**

---

### 🚀 Phương án 2: BỔ SUNG ĐẦY ĐỦ (1-2 tuần)

**Bao gồm Phương án 1 + Machine Learning + Graph Processing**

#### 4.2.1. Machine Learning Pipeline (3-4 ngày)

**Analysis mới: Analysis 10 - Review Sentiment Prediction**

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

@staticmethod
def train_sentiment_model(review_df):
    """
    10. Sentiment Analysis with MLlib
    Train model to predict positive/negative reviews
    """
    # Prepare data
    df = review_df.withColumn(
        "label",
        when(col("stars") >= 4, 1.0).otherwise(0.0)
    ).select("text", "label")

    # Split train/test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # Build pipeline
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=10000)
    idf = IDF(inputCol="raw_features", outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.01)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    # Train
    model = pipeline.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator()
    auc = evaluator.evaluate(predictions)

    print(f"Model AUC: {auc}")

    # Save model
    model.write().overwrite().save("models/sentiment_model")

    return model, auc

@staticmethod
def predict_sentiment(model, review_df):
    """Apply model to new data"""
    predictions = model.transform(review_df)
    return predictions.select(
        "business_id",
        "text",
        "probability",
        "prediction"
    )
```

**Kỹ năng thể hiện:**
- ✅ Feature engineering (Tokenizer, TF-IDF)
- ✅ ML Pipeline
- ✅ Model training và evaluation
- ✅ Model persistence

---

#### 4.2.2. Graph Processing (3-4 ngày)

**Analysis mới: Analysis 11 - User-Business Network**

```python
from graphframes import GraphFrame

@staticmethod
def user_business_network(review_df, business_df, user_df):
    """
    11. Social Network Analysis
    Build user-business bipartite graph
    """
    # Create vertices
    business_vertices = business_df.select(
        col("business_id").alias("id"),
        lit("business").alias("type"),
        col("name")
    )

    user_vertices = user_df.select(
        col("user_id").alias("id"),
        lit("user").alias("type"),
        col("name")
    )

    vertices = business_vertices.union(user_vertices)

    # Create edges (reviews as edges)
    edges = review_df.select(
        col("user_id").alias("src"),
        col("business_id").alias("dst"),
        col("stars").alias("weight")
    )

    # Build graph
    graph = GraphFrame(vertices, edges)

    # PageRank (influential businesses)
    pagerank = graph.pageRank(resetProbability=0.15, maxIter=10)
    influential_businesses = pagerank.vertices.filter(
        col("type") == "business"
    ).orderBy(desc("pagerank")).limit(20)

    # Connected components (user communities)
    communities = graph.connectedComponents()

    # Degree analysis
    in_degrees = graph.inDegrees  # Number of reviews per business
    out_degrees = graph.outDegrees  # Number of reviews per user

    return {
        'graph': graph,
        'influential_businesses': influential_businesses,
        'communities': communities,
        'in_degrees': in_degrees,
        'out_degrees': out_degrees
    }
```

---

#### 4.2.3. Advanced Statistics (2 ngày)

```python
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

@staticmethod
def correlation_analysis(business_df, review_df):
    """
    12. Statistical Analysis
    Correlation between features
    """
    # Join and prepare features
    joined = business_df.join(
        review_df.groupBy("business_id").agg(
            avg("stars").alias("avg_review_stars"),
            count("review_id").alias("review_count")
        ),
        "business_id"
    )

    # Assemble features
    assembler = VectorAssembler(
        inputCols=["stars", "review_count", "avg_review_stars"],
        outputCol="features"
    )

    feature_df = assembler.transform(joined)

    # Correlation matrix
    correlation_matrix = Correlation.corr(feature_df, "features", "pearson")

    return correlation_matrix

@staticmethod
def anomaly_detection(review_df):
    """
    Detect anomalous reviews (outliers)
    """
    # Calculate statistics
    stats = review_df.groupBy("business_id").agg(
        avg("stars").alias("mean_stars"),
        stddev("stars").alias("std_stars")
    )

    # Join back and find outliers (> 3 std deviations)
    with_stats = review_df.join(stats, "business_id")

    anomalies = with_stats.withColumn(
        "z_score",
        abs((col("stars") - col("mean_stars")) / col("std_stars"))
    ).filter(col("z_score") > 3)

    return anomalies
```

---

#### 4.2.4. Time Series Advanced (2 ngày)

```python
@staticmethod
def time_series_analysis(review_df):
    """
    13. Advanced Time Series
    Trend, seasonality, forecasting
    """
    # Aggregate by date
    daily_reviews = review_df.groupBy(
        to_date(col("date"), "yyyy-MM-dd HH:mm:ss").alias("date")
    ).agg(
        count("review_id").alias("count"),
        avg("stars").alias("avg_stars")
    ).orderBy("date")

    # Moving average (7-day)
    windowSpec = Window.orderBy("date").rowsBetween(-6, 0)

    with_ma = daily_reviews.withColumn(
        "ma_7day",
        avg("count").over(windowSpec)
    )

    # Trend detection (linear regression slope)
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import VectorAssembler

    # Convert date to numeric
    with_numeric = with_ma.withColumn(
        "days_since_start",
        datediff(col("date"), lit("2020-01-01")).cast("double")
    )

    assembler = VectorAssembler(
        inputCols=["days_since_start"],
        outputCol="features"
    )

    feature_df = assembler.transform(with_numeric).select(
        "features",
        col("count").alias("label")
    )

    lr = LinearRegression(maxIter=10)
    model = lr.fit(feature_df)

    trend_slope = model.coefficients[0]
    print(f"Trend: {'Increasing' if trend_slope > 0 else 'Decreasing'}")

    return with_ma, trend_slope
```

**Tổng thời gian Phương án 2: 1-2 tuần**

---

## 5. ROADMAP TRIỂN KHAI

### 📅 Timeline đề xuất

#### ⚡ Sprint 1: Quick Wins (Tuần 1)
- **Day 1-2:** Window Functions (Analysis 8: Trending Businesses)
- **Day 3:** Broadcast Join + Pivot/Unpivot (Analysis 9: Performance Matrix)
- **Day 4-5:** UDF + Pandas UDF + Testing

**Deliverable:** 2 analyses mới + code refactor

#### 🚀 Sprint 2: ML & Advanced (Tuần 2-3)
- **Week 2:**
  - Day 1-3: Machine Learning Pipeline (Analysis 10: Sentiment)
  - Day 4-5: Advanced Statistics (Analysis 12: Correlation)
- **Week 3:**
  - Day 1-3: Graph Processing (Analysis 11: Network)
  - Day 4-5: Time Series (Analysis 13: Forecasting)

**Deliverable:** 4 analyses mới + ML models

---

### 📁 Cấu trúc code mới

```
Spark_Batch/
├── batch_analytics.py              (hiện tại: 7 analyses)
├── batch_analytics_advanced.py     (MỚI: 6 analyses mới)
│   ├── Analysis 8: Trending (Window)
│   ├── Analysis 9: Pivot Matrix
│   ├── Analysis 10: ML Sentiment
│   ├── Analysis 11: Graph Network
│   ├── Analysis 12: Statistics
│   └── Analysis 13: Time Series
│
├── batch_udf.py                    (MỚI: UDF functions)
├── batch_ml.py                     (MỚI: ML utilities)
└── batch_graph.py                  (MỚI: Graph utilities)
```

---

### ✅ Checklist hoàn thành

**Sau khi triển khai Phương án 1 (Quick):**
- [ ] Window Functions: `row_number`, `rank`, `lag`, `lead`
- [ ] Pivot/Unpivot operations
- [ ] Broadcast join explicit
- [ ] Regular UDF
- [ ] Pandas UDF (vectorized)
- [ ] Skewed join handling

**Sau khi triển khai Phương án 2 (Full):**
- [ ] ML Pipeline với MLlib
- [ ] Feature engineering
- [ ] Model training + evaluation
- [ ] Graph processing với GraphFrames
- [ ] PageRank, Connected Components
- [ ] Correlation analysis
- [ ] Anomaly detection
- [ ] Time series với trend/seasonality

---

## 6. ĐÁNH GIÁ TỔNG KẾT

### Điểm số hiện tại theo từng tiêu chí:

| Tiêu chí | Điểm hiện tại | Điểm sau PA1 | Điểm sau PA2 |
|----------|---------------|--------------|--------------|
| 1. Tập hợp phức tạp | 40% ⭐⭐ | 80% ⭐⭐⭐⭐ | 90% ⭐⭐⭐⭐⭐ |
| 2. Biến đổi nâng cao | 50% ⭐⭐⭐ | 75% ⭐⭐⭐⭐ | 85% ⭐⭐⭐⭐ |
| 3. Join operations | 40% ⭐⭐ | 75% ⭐⭐⭐⭐ | 85% ⭐⭐⭐⭐ |
| 4. Tối ưu hóa | 50% ⭐⭐⭐ | 65% ⭐⭐⭐ | 80% ⭐⭐⭐⭐ |
| 5. Streaming | 60%* ⭐⭐⭐ | 60% ⭐⭐⭐ | 75% ⭐⭐⭐⭐ |
| 6. Phân tích nâng cao | 10% ⭐ | 30% ⭐⭐ | 85% ⭐⭐⭐⭐ |
| **TỔNG** | **42%** | **64%** | **83%** |

*Note: Streaming score chỉ tính cho nhánh HAI

### Khuyến nghị:

1. **Ngắn hạn (1 tuần):** Triển khai Phương án 1 để đạt 64%
   - Đủ để pass yêu cầu "trung cấp"
   - Thời gian hợp lý
   - ROI cao

2. **Trung hạn (2-3 tuần):** Triển khai Phương án 2 để đạt 83%
   - Xuất sắc cho yêu cầu "trung cấp"
   - Có thể lên "nâng cao"
   - Impressive cho reviewer

3. **Priority order:**
   - ✅ Window Functions (cần thiết nhất)
   - ✅ Broadcast Join (dễ nhất, tác động lớn)
   - ✅ UDF/Pandas UDF (practical)
   - ✅ ML Pipeline (impressive nhất)

---

**Bạn muốn tôi bắt đầu triển khai phương án nào? Tôi khuyến nghị bắt đầu với Phương án 1! 🚀**
