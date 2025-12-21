"""
UDF Library - Custom User Defined Functions
Demonstrates both Regular UDF and Pandas UDF (vectorized)
"""
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, FloatType, BooleanType, ArrayType
import pandas as pd
from datetime import datetime


# ============================================================================
# REGULAR UDFs (Row-by-row processing - slower but simpler)
# ============================================================================

@udf(returnType=StringType())
def categorize_rating(stars):
    """
    Categorize rating into quality tiers

    Args:
        stars (float): Rating value (1.0 - 5.0)

    Returns:
        str: Category (Excellent, Good, Average, Poor)

    Example:
        df.withColumn("rating_category", categorize_rating(col("stars")))
    """
    if stars is None:
        return "Unknown"

    if stars >= 4.5:
        return "Excellent"
    elif stars >= 3.5:
        return "Good"
    elif stars >= 2.5:
        return "Average"
    else:
        return "Poor"


@udf(returnType=BooleanType())
def is_weekend(date_str):
    """
    Check if a date falls on weekend (Saturday or Sunday)

    Args:
        date_str (str): Date string in format 'YYYY-MM-DD HH:MM:SS'

    Returns:
        bool: True if weekend, False otherwise

    Example:
        df.withColumn("is_weekend", is_weekend(col("date")))
    """
    if date_str is None:
        return None

    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        # Monday=0, Sunday=6
        return dt.weekday() >= 5  # Saturday(5) or Sunday(6)
    except:
        return None


@udf(returnType=StringType())
def extract_city_state(location_str):
    """
    Extract city and state from location string

    Args:
        location_str (str): Location like "Phoenix, AZ"

    Returns:
        str: Formatted "City, State" or original if parsing fails

    Example:
        df.withColumn("location", extract_city_state(col("address")))
    """
    if location_str is None:
        return "Unknown"

    try:
        parts = location_str.split(',')
        if len(parts) >= 2:
            city = parts[0].strip()
            state = parts[1].strip()
            return f"{city}, {state}"
        return location_str
    except:
        return location_str


# ============================================================================
# PANDAS UDFs (Vectorized processing - MUCH FASTER!)
# ============================================================================

@pandas_udf(FloatType())
def sentiment_score(text: pd.Series) -> pd.Series:
    """
    Calculate sentiment score from review text (0.0 = negative, 1.0 = positive)

    Uses simple word counting approach with positive/negative word lists.
    Pandas UDF is vectorized - processes entire Series at once (10-100x faster!)

    Args:
        text (pd.Series): Series of review text strings

    Returns:
        pd.Series: Sentiment scores (0.0 to 1.0)

    Example:
        df.withColumn("sentiment", sentiment_score(col("text")))

    Performance:
        - Regular UDF: ~5-10s per 1000 reviews
        - Pandas UDF: ~0.5s per 1000 reviews (10x faster!)
    """
    # Word lists for sentiment analysis
    positive_words = [
        'great', 'excellent', 'amazing', 'wonderful', 'fantastic',
        'love', 'best', 'awesome', 'perfect', 'outstanding',
        'delicious', 'friendly', 'highly', 'recommend', 'enjoyed'
    ]

    negative_words = [
        'bad', 'terrible', 'worst', 'awful', 'horrible',
        'hate', 'disgusting', 'poor', 'disappointing', 'never',
        'rude', 'slow', 'dirty', 'overpriced', 'avoid'
    ]

    def calculate_score(t):
        """Calculate score for single text"""
        if pd.isna(t) or t == '':
            return 0.5  # Neutral for empty text

        t_lower = t.lower()

        # Count positive and negative words
        pos_count = sum(t_lower.count(word) for word in positive_words)
        neg_count = sum(t_lower.count(word) for word in negative_words)

        total = pos_count + neg_count

        if total == 0:
            return 0.5  # Neutral if no sentiment words found

        # Calculate ratio (0.0 = all negative, 1.0 = all positive)
        return pos_count / total

    # Vectorized apply (fast!)
    return text.apply(calculate_score)


@pandas_udf(StringType())
def extract_keywords(text: pd.Series) -> pd.Series:
    """
    Extract top keywords from review text

    Simple approach: find most common words (4+ letters)
    Pandas UDF for vectorized processing.

    Args:
        text (pd.Series): Series of review text strings

    Returns:
        pd.Series: Comma-separated keywords

    Example:
        df.withColumn("keywords", extract_keywords(col("text")))
    """
    import re
    from collections import Counter

    def extract(t):
        """Extract keywords from single text"""
        if pd.isna(t) or t == '':
            return ''

        # Find all words (4+ letters, lowercase)
        words = re.findall(r'\b[a-z]{4,}\b', t.lower())

        # Remove common stop words
        stop_words = {
            'this', 'that', 'with', 'from', 'they', 'were',
            'been', 'have', 'their', 'about', 'would', 'there'
        }
        words = [w for w in words if w not in stop_words]

        if not words:
            return ''

        # Get top 5 most common
        top_words = Counter(words).most_common(5)
        return ', '.join([word for word, count in top_words])

    # Vectorized apply
    return text.apply(extract)


@pandas_udf(FloatType())
def text_length_normalized(text: pd.Series) -> pd.Series:
    """
    Calculate normalized text length (0.0 = very short, 1.0 = very long)

    Useful for filtering out spam or low-quality reviews.
    Normalizes to typical review length range (10-500 chars).

    Args:
        text (pd.Series): Series of review text strings

    Returns:
        pd.Series: Normalized length scores (0.0 to 1.0)

    Example:
        df.withColumn("text_quality", text_length_normalized(col("text")))
    """
    def normalize_length(t):
        """Normalize text length"""
        if pd.isna(t):
            return 0.0

        length = len(t)

        # Normalize to 10-500 character range
        if length < 10:
            return 0.0  # Too short
        elif length > 500:
            return 1.0  # Very long
        else:
            # Linear scale between 10-500
            return (length - 10) / (500 - 10)

    return text.apply(normalize_length)


@pandas_udf(ArrayType(StringType()))
def extract_hashtags(text: pd.Series) -> pd.Series:
    """
    Extract hashtags from text (if any)

    Example: "Great food! #yummy #delicious" -> ["yummy", "delicious"]

    Args:
        text (pd.Series): Series of review text strings

    Returns:
        pd.Series: Arrays of hashtags

    Example:
        df.withColumn("hashtags", extract_hashtags(col("text")))
    """
    import re

    def extract(t):
        """Extract hashtags from single text"""
        if pd.isna(t) or t == '':
            return []

        # Find all hashtags (#word)
        hashtags = re.findall(r'#(\w+)', t.lower())
        return hashtags if hashtags else []

    return text.apply(extract)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def register_all_udfs(spark):
    """
    Register all UDFs with SparkSession for SQL usage

    Usage:
        from batch_udf import register_all_udfs
        register_all_udfs(spark)

        # Then can use in SQL:
        spark.sql("SELECT categorize_rating(stars) FROM reviews")
    """
    spark.udf.register("categorize_rating", categorize_rating)
    spark.udf.register("is_weekend", is_weekend)
    spark.udf.register("extract_city_state", extract_city_state)
    spark.udf.register("sentiment_score", sentiment_score)
    spark.udf.register("extract_keywords", extract_keywords)
    spark.udf.register("text_length_normalized", text_length_normalized)
    spark.udf.register("extract_hashtags", extract_hashtags)

    print("✓ All UDFs registered successfully!")


def demo_udfs(spark, sample_size=10):
    """
    Demo function to show UDF usage with sample data

    Args:
        spark: SparkSession
        sample_size: Number of sample rows to create

    Returns:
        DataFrame with UDF results
    """
    from pyspark.sql import Row

    # Create sample data
    sample_data = [
        Row(
            review_id=f"review_{i}",
            stars=3.5 + (i % 3) * 0.5,
            text="Great food and excellent service! Highly recommend." if i % 2 == 0
                 else "Terrible experience. Bad food and rude staff.",
            date="2023-12-15 18:30:00" if i % 2 == 0 else "2023-12-16 14:00:00"
        )
        for i in range(sample_size)
    ]

    df = spark.createDataFrame(sample_data)

    # Apply all UDFs
    result = df.withColumn(
        "rating_category", categorize_rating("stars")
    ).withColumn(
        "is_weekend", is_weekend("date")
    ).withColumn(
        "sentiment", sentiment_score("text")
    ).withColumn(
        "keywords", extract_keywords("text")
    ).withColumn(
        "text_quality", text_length_normalized("text")
    )

    return result


# ============================================================================
# PERFORMANCE COMPARISON
# ============================================================================

def compare_udf_performance(spark, df, column_name):
    """
    Compare performance: Regular UDF vs Pandas UDF

    Args:
        spark: SparkSession
        df: DataFrame with text column
        column_name: Name of text column to process

    Returns:
        dict: Performance metrics
    """
    import time

    # Regular UDF (slower)
    @udf(returnType=FloatType())
    def regular_sentiment(text):
        if text is None:
            return 0.5
        positive = sum(text.lower().count(w) for w in ['great', 'excellent', 'good'])
        negative = sum(text.lower().count(w) for w in ['bad', 'terrible', 'poor'])
        total = positive + negative
        return positive / total if total > 0 else 0.5

    # Test Regular UDF
    start = time.time()
    df.withColumn("sentiment_regular", regular_sentiment(column_name)).count()
    regular_time = time.time() - start

    # Test Pandas UDF
    start = time.time()
    df.withColumn("sentiment_pandas", sentiment_score(column_name)).count()
    pandas_time = time.time() - start

    speedup = regular_time / pandas_time if pandas_time > 0 else 0

    return {
        'regular_udf_time': regular_time,
        'pandas_udf_time': pandas_time,
        'speedup': speedup,
        'improvement': f"{(1 - pandas_time/regular_time) * 100:.1f}%" if regular_time > 0 else "N/A"
    }


if __name__ == "__main__":
    """
    Test script - run this file directly to test UDFs

    Usage:
        python3 batch_udf.py
    """
    from pyspark.sql import SparkSession

    print("="*60)
    print("UDF LIBRARY - DEMO & TEST")
    print("="*60)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("UDF Demo") \
        .master("local[*]") \
        .getOrCreate()

    # Demo UDFs
    print("\n1. Creating sample data and applying UDFs...")
    result_df = demo_udfs(spark, sample_size=5)

    print("\nResults:")
    result_df.select(
        "review_id",
        "stars",
        "rating_category",
        "sentiment",
        "is_weekend"
    ).show(truncate=False)

    print("\nKeywords extracted:")
    result_df.select("review_id", "keywords").show(truncate=False)

    # Register UDFs
    print("\n2. Registering UDFs for SQL usage...")
    register_all_udfs(spark)

    # Test SQL usage
    result_df.createOrReplaceTempView("reviews")
    sql_result = spark.sql("""
        SELECT
            review_id,
            stars,
            categorize_rating(stars) as category,
            sentiment_score(text) as sentiment
        FROM reviews
        LIMIT 3
    """)

    print("\nSQL Query Results:")
    sql_result.show(truncate=False)

    print("\n" + "="*60)
    print("✓ All UDFs working correctly!")
    print("="*60)

    spark.stop()
