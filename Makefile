
add-file:
	docker cp dataset/trips.csv hadoop-namenode:/
	docker exec hadoop-namenode bash -c hadoop dfs -mkdir -p hdfs:///data/landing/tripdata/
	docker exec hadoop-namenode bash -c "hadoop fs -copyFromLocal /trips.csv hdfs:///data/landing/tripdata/trips.csv"
