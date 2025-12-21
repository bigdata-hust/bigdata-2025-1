import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, HashingTF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml.fpm import FPGrowth
import time
import traceback
from pyspark.sql.functions import when
from functools import partial
from configuration import SparkConfig
import os

kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092") 
spark = SparkConfig
review = spark.read.format("kafka").option("kafka.bootstrap.server" , kafka_broker)  \
                                    .option("startingOffsets" , "earnliest") \
                                    .option("subcribe" , "review") \
                                    .load() 
review.show(5)

review_label = review.withColumn(
    "label",
    when(review.stars >= 4, 1)
    .when(review.stars <= 2, 0)
    .otherwise(2)
)

tokenizer = Tokenizer(
    inputCol="text",
    outputCol="words"
)

stopwords = StopWordsRemover(
    inputCol="words",
    outputCol="filtered"
)

hashingTF = HashingTF(
    inputCol="filtered",
    outputCol="rawFeatures",
    numFeatures=20000
)

idf = IDF(
    inputCol="rawFeatures",
    outputCol="features"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=20,
    regParam=0.01
)

pipeline = Pipeline(stages=[
    tokenizer,
    stopwords,
    hashingTF,
    idf,
    lr
])

train_df, test_df = review_label.randomSplit([0.8, 0.2], seed=42)

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

predictions.select("text", "label", "prediction").show(5)

model.write().overwrite().save("../models/tfidf_sentiment")
