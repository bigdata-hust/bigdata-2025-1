from flask import Flask, request, render_template
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
import os
spark = SparkSession.builder \
    .appName("SentimentFlask") \
    .master("local[*]") \
    .getOrCreate()

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
    app.run(debug=True, port=5000)
