from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("preprocess")\
                .getOrCreate()

namenode = "hadoop-namenode:8020"

## Main table without transformation
trips_df = spark.read.format("parquet").load(f"hdfs://{namenode}/data/bronze/tripdata/")


#First transformation of the date
preprocess_data = trips_df.withColumn("datetime", date_format("datetime", "yyyy-MM-dd HH:mm"))


#First task: trips with similar origin, destination and time of day should be grouped together
preprocess_data_time_of_day = trips_df.withColumn("time_of_day", date_format("datetime", "HH:mm"))

grouped_df = preprocess_data_time_of_day.groupBy("region", "origin_coord", "destination_coord", "time_of_day").agg({"datasource": "count"}).withColumnRenamed("count(datasource)", "trips")

grouped_df.write.format("parquet").mode("append").save(f"hdfs://{namenode}/data/silver/tripdata/")


# Second task: Develop a way to obtain the weekly average number of trips for an area defined by a region
grouped_df_avg_weeks = preprocess_data.groupBy("region", year("datetime").alias("year"), weekofyear("datetime").alias("week")).agg({"datasource": "count"}).withColumnRenamed("count(datasource)", "trips")

num_weeks = grouped_df_avg_weeks.select("year", "week").distinct().count()

avg_trips = grouped_df.groupBy("region").agg({"trips": "sum"}).withColumnRenamed("sum(trips)", "total_trips").withColumn("avg_trips", col("total_trips") / num_weeks)

avg_trips.write.format("parquet").mode("overwrite").save(f"hdfs://{namenode}/data/silver/avg_trips_region/")


#Delivering the raw-data
preprocess_data.write.format("parquet").mode("append").save(f"hdfs://{namenode}/data/silver/rawtripdata/")