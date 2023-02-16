from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

namenode = "hadoop-namenode:8020"

#First task

preprocess_trips = spark.read.format("parquet").load(f"hdfs://{namenode}/data/silver/tripdata/")

preprocess_trips.write.format("jdbc").mode("append").option("url", f"jdbc:postgresql://jobsity-postgres:5432/jobsity").option("user", "jobsity") \
    .option("password", "jobsity") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "staging_tripdata") \
    .save()

print("finished first task")

#Second task


preprocess_avg_trips_region = spark.read.format("parquet").load(f"hdfs://{namenode}/data/silver/avg_trips_region/")

preprocess_avg_trips_region.write.format("jdbc").mode("append").option("url", f"jdbc:postgresql://jobsity-postgres:5432/jobsity").option("user", "jobsity") \
    .option("password", "jobsity") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "avg_trips_region") \
    .save()

print("finished second task")

#delivering raw-data

raw_data_trips = spark.read.format("parquet").load(f"hdfs://{namenode}/data/silver/rawtripdata/")

raw_data_trips.write.format("jdbc").mode("append").option("url", f"jdbc:postgresql://jobsity-postgres:5432/jobsity").option("user", "jobsity") \
    .option("password", "jobsity") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "raw_tripsdata") \
    .save()

print("done")