# main.py
from spark_config import get_spark_session
from data_processing import get_peak_hours, get_top_categories, get_store_stats

def main():
    spark = get_spark_session("YelpDataAnalysis")

    # Đọc dữ liệu
    business_df = (
    spark.read
    .option("multiline", "true")
    .option("mode", "PERMISSIVE")
    .json("data/business.json")
)
    review_df = (
    spark.read
    .option("multiline", "true")  # cho phép đọc JSON dạng danh sách
    .option("mode", "PERMISSIVE") # cho phép bỏ qua lỗi nhẹ
    .json("data/review.json")
)

    # 5️⃣ Khung giờ mua hàng cao điểm
    print("\n===== 5. Khung giờ mua hàng cao điểm =====")
    peak_hours_df = get_peak_hours(review_df)
    peak_hours_df.show(10)

    # 6️⃣ Top bán chạy theo danh mục
    print("\n===== 6. Top danh mục bán chạy =====")
    top_cat_df = get_top_categories(business_df, review_df)
    top_cat_df.show(10, truncate=False)

    # 7️⃣ Thông số cửa hàng cụ thể
    print("\n===== 7. Thông tin cửa hàng =====")
    store_stats_df = get_store_stats(business_df, review_df)
    store_stats_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
