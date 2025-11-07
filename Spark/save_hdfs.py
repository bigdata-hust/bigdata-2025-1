import pyspark
"""
Yelp Big Data Analysis System
Optimized PySpark Pipeline for Large-Scale Data Processing
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
import time
from datetime import datetime


spark = SparkSession.builder.config('spark.driver.memory' , '8g').getOrCreate()

def save_data_hdfs(result , name) :
    print('=' * 50)
    print(f'Saving {name} to HDFS ...')
    output = 'hdfs://localhost:9000/bigdata/features/' + name
    # Add columns created_date and updated_date
    result_date = result.withColumn('created_date' , current_timestamp()) \
                        .withColumn('updated_date' , current_timestamp())
    # Partition by year , month , day
    result_partitioned = result_date.withColumn('year' , year(col('created_date'))) \
                                    .withColumn('month' , month(col('created_date'))) \
                                    .withColumn('day' , dayofmonth(col('created_date'))) \
                                    .withColumn('hour' , hour(col('created_date'))) 
    # Save to HDFS
    result_partitioned.write.partitionBy('year' , 'month' , 'day' , 'hour').mode('overwrite').parquet(output)
    print(f'Data saved to HDFS at {output}')

list = os.listdir('home/mhai/input_yelp')
for file in list :
    df = spark.read.parquet('home/mhai/input_yelp/' + file)
    name = file.split('.')[0]
    save_data_hdfs(df , name) 


        
