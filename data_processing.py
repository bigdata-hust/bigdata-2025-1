
from pyspark.sql.functions import (
    to_date, year, month, count, desc, col, explode, split, expr
)
def get_peak_hours(review_df):
    """
    Phân tích số lượng review theo ngày/tháng/năm.
    """
    df = review_df.withColumn("date_parsed", to_date(col("date"), "M/d/yyyy"))
    return (
        df.groupBy(year("date_parsed").alias("year"), month("date_parsed").alias("month"))
          .count()
          .orderBy(desc("count"))
    )


def get_top_categories(business_df, review_df):
    """
    Phân tích top danh mục (category) bán chạy - dựa trên số lượng review.
    """
    df_business = business_df.withColumn("category", explode(split(col("categories"), ", ")))
    joined = review_df.join(df_business, "business_id")
    return joined.groupBy("category").count().orderBy(desc("count"))



def get_store_stats(business_df, review_df):
    """
    Trả về thống kê thông tin của TẤT CẢ cửa hàng (không cần lọc từng ID).
    Gồm: tên, danh mục, số sao, tổng review,...
    """
    # Tính tổng số review cho mỗi cửa hàng
    review_stats = (
        review_df.groupBy("business_id")
        .agg(count("*").alias("total_reviews"))
    )

    # Gộp với thông tin cửa hàng
    return (
        review_stats.join(
            business_df.select("business_id", "name", "categories", "stars", "review_count"),
            "business_id",
            "left"
        )
        .orderBy("business_id")
    )

