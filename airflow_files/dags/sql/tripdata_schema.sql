-- create trips table
CREATE TABLE IF NOT EXISTS tripdata (
    region VARCHAR NOT NULL,
    origin_coord geometry(Point, 4326) NOT NULL,
    destination_coord geometry(Point, 4326) NOT NULL,
    time_of_day varchar NOT NULL,
    trips BIGINT NOT NULL);