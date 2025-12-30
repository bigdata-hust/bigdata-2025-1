from flask import Flask, request, render_template
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
import os
class SparkConfig:
    @staticmethod
    def _get_env(key: str, default: str) -> str:
        """Lấy biến môi trường hoặc dùng giá trị mặc định"""
        return os.environ.get(key, default)

    @staticmethod
    def create_spark_session():
        # --- 1. LẤY CẤU HÌNH KẾT NỐI TỪ MÔI TRƯỜNG ---
        spark_master = SparkConfig._get_env("SPARK_MASTER_URL", "spark://spark-master:7077")
        hdfs_uri = SparkConfig._get_env("HDFS_URI", "hdfs://127.0.0.1:9000")

        # --- 2. KHỞI TẠO BUILDER ---
        builder = (
            SparkSession.builder
            .appName("Yelp Big Data Analysis System")
            .master(spark_master) # Ép dùng Standalone, không dùng YARN nên sẽ hết lỗi
            
            # ---- HADOOP / HDFS CONFIG ----
            .config("spark.hadoop.fs.defaultFS", hdfs_uri)
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

            # ---- MEMORY (Giữ nguyên cấu hình cũ của bạn) ----
            .config("spark.driver.memory", "8g")

            .config("spark.executor.memory", "10g")


            .config("spark.executor.cores", "3")


            .config("spark.executor.memoryOverhead", "8g")


            .config("spark.executor.instances", "2")

            .config("spark.dynamicAllocation.enabled", "true") 


            .config("spark.shuffle.service.enabled", "false")
            .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
            .config(
                "spark.executor.extraJavaOptions",
                "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"
            )

            .config("spark.driver.port", "7078")
            .config("spark.driver.blockManager.port", "7079")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.host", SparkConfig._get_env("SPARK_DRIVER_HOST", "0.0.0.0"))
            .config("spark.driver.maxResultSize", "2g")
         
            .config("spark.sql.shuffle.partitions", "8")   
            .config("spark.default.parallelism", "8")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.adaptive.enabled", "true")
            
            
            
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")


            .config("spark.sql.adaptive.skewJoin.enabled", "true")
        )
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        return spark
spark = SparkConfig.create_spark_session()

hdfs_host = os.getenv("HDFS_URI" , "hdfs://hdfs-namenode:9000")
model = PipelineModel.load(f"{hdfs_host}/yelp-sentiment/models/tfidf_sentiment")

app = Flask(__name__)

label_map = {1: "Positive", 0: "Negative" }

@app.route("/", methods=["GET", "POST"])
def home(): 
    prediction = None

    if request.method == "POST":
        text = request.form["text"]
        df = spark.createDataFrame([(text,)], ["text"])
        pred_row = model.transform(df).collect()[0]
        pred_index = int(pred_row["prediction"])
        prediction = label_map[pred_index]

    return render_template("index.html", prediction=prediction)

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/charts")
def charts():
    image_files = [
        ("Sentiment Analysis Overview", "static/analysis_sentiment.png"),
        ("Top Categories", "static/categories_top.png"),
        ("Distribution Overview", "static/distribution.png"),
        ("Number of Entities", "static/number_entities.png"),
        ("Reviews Per Year", "static/review_per_year.png"),
        ("Top Categories Breakdown", "static/top_categories.png"),
        ("Top Rated Businesses", "static/top_rated_business.png"),
        ("Top Words Present", "static/top_word_present.png"),
    ]

    return render_template("charts.html", images=image_files)


if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True, port=5000)
