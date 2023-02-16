# data-engineering-challenge

This is a local data pipeline that enables the processing of geospatial data using modern big data technologies. The pipeline is built with Apache Airflow, Apache Spark, Hadoop Distributed File System (HDFS), and PostgreSQL. It consists of an Airflow DAG that automatically triggers the data processing whenever new files are uploaded to HDFS.

The data is first ingested into the HDFS data lake and is initially processed in a landing zone. From there, the data moves to a second layer where it is stored in a more optimized format using Parquet files. In the third layer, data validation and cleansing rules are applied to ensure the quality of the data. Finally, the cleansed data is moved to a staging table in PostgreSQL for analytical data storage, where it undergoes further transformations before being moved to a final table with the coordinate columns parsed into PostGIS type columns.

To easily access the processed data, the pipeline also features a Flask API that exposes,
the data stored in PostgreSQL.

# How to use

1. Make sure you have [git](https://git-scm.com/downloads) installed.
2. Make sure you have [Docker](https://docs.docker.com/engine/install/) installed.
3. Clone the project:

````
git clone https://github.com/GiordaniAndre/data-engineering-challenge.git
````

4. Open you terminal in linux / prompt in windows
5. Drive into the data-engineering-challenge directory:

````
cd data-engineering-challenge
````

6. Run the commands to start the containers (make sure you are in the same directory of docker-compose.yaml):

````
docker-compose up
````
7. In the config, airflow is running in the port 8080 with user admin and password admin.
8. Find the DAG trips-processing and click on the Play Button and after in Trigger Dag.
9. Wait for the DAG sensor starts to listen to the HDFS directory.
10. If you are using Linux, open another terminal, drive into the data-engineering-challenge folder and run the command:
````
make add-file
````
If you are using Windows, drive into the data-engineering-challenge and run the .bat:
````
add-file.bat
````

Or just run the commands in your terminal:
````
docker cp dataset\trips.csv hadoop-namenode:\
docker exec hadoop-namenode powershell.exe -Command "hadoop dfs -mkdir -p hdfs:///data/landing/tripdata/"
docker exec hadoop-namenode powershell.exe -Command "hadoop fs -copyFromLocal /trips.csv hdfs:///data/landing/tripdata/trips.csv"

````

11. The DAG will execute in the following sequence:
````
                  +-----------------------+
                  |                       |
                  |   landing_zone_job    |
                  |                       |
                  +-----------+-----------+
                              |
                              |
                              |
                  +-----------v-----------+
                  |                       |
                  |second_layer_processing|
                  |                       |
                  +-----------+-----------+
                              |
                              |
                              |
                  +-----------v-----------+
                  |                       |
                  | third_layer_processing|
                  |                       |
                  +-----------+-----------+
                              |
                              |
                              |
                  +-----------v-----------+
                  |                       |
                  | postgre_ingestion_job |
                  |                       |
                  +-----------+-----------+
                              |
                              |
                              |
                  +-----------v------------+
                  |                        |
                  |  trip_table_creation   |
                  |                        |
                  +------------+-----------+
                               |
                               |
                               |
                  +------------v-----------+
                  |                        |
                  |   trip_table_loading   |
                  |                        |
                  +------------------------+


````

Wait for it to finish the execution.

12. To check the Spark UI go: [http://localhost:8888](http://localhost:8888)

13. Connecting in to the PostgreSQL:

````
docker exec -it jobsity-postgres bash
psql --username=jobsity
\c jobsity
select * from staging_tripdata limit 10;
select * from tripdata limit 10;
select * from raw_tripsdata limit 10;
````

14.Connect to the API to get weekly avg with a bounding box.

````
wget http://localhost:50555/
````
Also, there is a table that can calculate this in the processing time
````
select * from avg_trips_region;
````


## Services and ports used in the project

````
Service: api
  Exposed Port: 50555

Service: spark-master
  Exposed Port: 8888
  Internal Port: 8080
  Internal Port: 7077

Service: spark-worker
  Exposed Port: 8081
  Internal Port: 8081

Service: jobsity-postgres
  Exposed Port: 5433

Service: airflow-postgres
  Exposed Port: 5432

Service: redis
  Exposed Port: 6379

Service: webserver
  Exposed Port: 8080

Service: flower
  Exposed Port: 5555

Service: hadoop-namenode
  Exposed Port: 9870
  Exposed Port: 8020
  Exposed Port: 50070

Service: hadoop-datanode
  Internal Port: 9864
  Internal Port: 9866
  Internal Port: 9867
  Internal Port: 9868
  Internal Port: 9869
  Internal Port: 50010
  Internal Port: 50020
  Internal Port: 50075
  Internal Port: 50090

````


# To do:
````
1. Create a container with HashiCorp Vault and uses the vault to retrieve password for the server
2. Upgrade the format parquet into delta-table and use mergeSchema in the tables to update the tables
3. For simplicity, all the data is loaded at every execution, with delta-table we could upgrade to have a feature of incremental loads.
4. Implement in cloud infrastrucutre as we can see in the next section.
````

# AWS Infra

![Infrastructure on AWS](/img/aws.png)

The proposed infrastructure for the data pipeline consists of various AWS services. Airflow would be deployed on Amazon Elastic Container Service (ECS), which is a fully-managed container orchestration service that allows us to run, stop, and manage Docker containers on a cluster. Airflow can be deployed in a Docker container on ECS and managed using Amazon Elastic Kubernetes Service (EKS).

For storage, we would use Amazon Simple Storage Service (S3), which is an object storage service that offers industry-leading scalability, data availability, security, and performance. We can store the input data files on S3 and create an S3 notification event that will trigger the DAG when a new file is uploaded.

The database would be an Amazon Relational Database Service (RDS), which is a managed database service that makes it easy to set up, operate, and scale a relational database in the cloud. We can use PostgreSQL as the database for storing the processed data.

To cache and store the results of intermediate computations, we can use Amazon ElastiCache for Redis, which is an in-memory data store that provides low-latency access to frequently requested data.

For big data processing, we can use Amazon Elastic MapReduce (EMR), which is a fully-managed big data processing service that makes it easy to process large amounts of data using Spark, Hadoop, or other big data frameworks. We can create and destroy EMR clusters on the DAG to avoid having a Spark cluster always on, which can help reduce costs.

To trigger the DAG whenever a new file is uploaded to S3, we can create an S3 notification event using Amazon Simple Notification Service (SNS), which is a flexible, fully-managed pub/sub messaging and mobile notifications service for coordinating the delivery of messages to subscribing endpoints.

Overall, this infrastructure is highly scalable and flexible, making it well-suited to handle large volumes of data and accommodate future growth.


#Azure Infra

![Infrastructure on Azure](/img/Azure.png)

The proposed infrastructure on Azure would involve deploying Airflow on Azure Kubernetes Service (AKS), using Azure Blob Storage as the storage solution, and Azure Database for PostgreSQL as the database. Redis could be provisioned as a managed service, such as Azure Cache for Redis, and the Spark Cluster could be deployed on Azure HDInsight.

We could also use Azure Event Grid to trigger the DAG when a new file is uploaded to Blob Storage. This would involve creating a Blob Storage trigger that would send an event to Event Grid, which in turn could trigger the DAG.

Similar to AWS, we can also create and destroy HDInsight clusters as needed to avoid having a Spark cluster always on, and to minimize costs. This can be achieved by creating a DAG that spins up a new HDInsight cluster when needed and tears it down when it's no longer required. We can also make use of Azure Data Factory to orchestrate data movement and processing within the pipeline.

# Bonus features

• The solution is containerized

• There are two different cloud solutions

• There is a directory called SQL in the root of the project with both .sql files answering the questions:
````
From the two most commonly appearing regions, which is the latest datasource?

What regions has the "cheap_mobile" datasource appeared in?
````