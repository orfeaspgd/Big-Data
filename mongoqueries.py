import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, avg, countDistinct, approx_count_distinct, to_timestamp
from pyspark.sql.functions import from_json, col, count, sum, abs
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, DoubleType, IntegerType
from datetime import datetime
from pyspark.sql import functions as F
import os
from pymongo import MongoClient



# Initialize Spark session
if __name__ == "__main__":
    try:
            spark = SparkSession\
                .builder\
                .appName("MySparkSession")\
                .master("local")\
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.0')\
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/BigData.data") \
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/BigData.data") \
                .getOrCreate()
    except Exception as e:
        print("Error creating SparkSession", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(-1)

# Assuming tempRawData is already defined and loaded
# Example: rawData = spark.read.format("your_format").load("your_path")

    jsonSchema = StructType([
        StructField("name", StringType()),
        StructField("origin", StringType()),
        StructField("destination", StringType()),
        StructField("time", StringType()),
        StructField("link", StringType()),
        StructField("position", FloatType()),
        StructField("spacing", FloatType()),
        StructField("speed", FloatType())
    ])

    processedDataSchema = StructType([
        StructField("time", TimestampType(), True),
        StructField("link", StringType(), True),
        StructField("vcount", IntegerType()),
        StructField("vspeed", DoubleType()),
        StructField("name", StringType())
    ])

rawData = spark.read\
        .format("mongodb")\
        .option("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")\
        .option("spark.mongodb.read.database", "BigData")\
        .option("spark.mongodb.read.collection", "data")\
        .load()

processedData = spark.read\
        .format("mongodb")\
        .option("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")\
        .option("spark.mongodb.read.database", "BigData")\
        .option("spark.mongodb.read.collection", "processed")\
        .load()

filteredData = rawData.filter(col("time").between("2024-09-20T20:12:44.000+00:00", "2024-09-20T20:12:44.000+00:00"))

nodeQuery = filteredData.groupBy("link")\
                .agg(count("name").alias("vcount"))\
                .orderBy("vcount", ascending = True)\

speedQuery = filteredData.groupBy("link")\
                .agg(sum(abs(col("speed"))).alias("total_speed"))

routeQuery = filteredData.orderBy("position", ascending = False).limit(1).drop("_id", "destination", "link", "name", "origin", "spacing", "speed", "time")

resultsnodeQuery = nodeQuery.limit(1)

resultspeedQuery = nodeQuery.join(speedQuery, on="link")\
                  .withColumn("vspeed", F.round(col("total_speed") / col("vcount"), 2))\
                  .orderBy("vspeed", ascending=False)\
                  .drop("total_speed")\
                  .drop("vcount")\
                  .limit(1)


resultsnodeQuery.show()
resultspeedQuery.show()
routeQuery.show()
 
spark.stop()