from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, count

spark = SparkSession.builder \
    .appName("StructuredNetworkWordCount") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Đọc stream từ socket
df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Tách các từ ra
words = df.select(explode(split(col("value"), " ")).alias("word"))

# Đếm số lần xuất hiện từng từ
wordCounts = words.groupBy("word").count()

# Ghi kết quả ra console
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
