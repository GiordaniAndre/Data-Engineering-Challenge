from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("preprocess")\
                .getOrCreate()

namenode = "hadoop-namenode:8020"


trips_df = spark.read.csv(f"hdfs://{namenode}/data/landing/tripdata/", header=True, inferSchema=True)

trips_df.write \
    .format("parquet") \
    .mode("append") \
    .save(f"hdfs://{namenode}/data/bronze/tripdata/")

