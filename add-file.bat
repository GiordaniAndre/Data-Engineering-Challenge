@echo off
docker cp dataset\trips.csv hadoop-namenode:\
docker exec hadoop-namenode powershell.exe -Command "hadoop dfs -mkdir -p hdfs:///data/landing/tripdata/"
docker exec hadoop-namenode powershell.exe -Command "hadoop fs -copyFromLocal /trips.csv hdfs:///data/landing/tripdata/trips.csv"
