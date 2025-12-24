from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.storagelevel import StorageLevel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os

spark = (
    SparkSession.builder
    .appName("YelpSentimentOptimized")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "6g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.default.parallelism", "20")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

hdfs_host = os.getenv("HDFS_URI" , "hdfs://hdfs-namenode:9000")
review = spark.read.load(
    f"{hdfs_host}/yelp-sentiment/data/review"
)

review_label = (
    review
    .filter(col("stars") != 3)
    .withColumn(
        "label",
        when(col("stars") >= 4, 1).otherwise(0)
    )
    .select("text", "label")
)

train_df, test_df = review_label.randomSplit(
    [0.8, 0.2],
    seed=42
)

# train_df.persist()
# test_df.persist()

tokenizer = Tokenizer(
    inputCol="text",
    outputCol="words"
)

stopwords = StopWordsRemover(
    inputCol="words",
    outputCol="filtered"
)

vectorizer = CountVectorizer(
    inputCol="filtered",
    outputCol="features",
    vocabSize=10000,   
    minDF=5            
)


lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=10,
    regParam=0.1,
    elasticNetParam=0.0
)


pipeline = Pipeline(stages=[
    tokenizer,
    stopwords,
    vectorizer,
    lr
])


print(" Training model ...")
model = pipeline.fit(train_df)


predictions = model.transform(test_df)


evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
print(f" AUC = {auc:.4f}")

model.write().overwrite().save(
    f"{hdfs_host}/yelp-sentiment/models/tfidf_sentiment"
)

print(" Model saved successfully")
