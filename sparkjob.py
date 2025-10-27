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


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Wrong number of args.", file=sys.stderr)
        sys.exit(-1)

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


    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

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

    try:
            lines = (spark\
                .read\
                .format("kafka")\
                .option("kafka.bootstrap.servers", bootstrapServers)\
                .option(subscribeType, topics)\
                .load()\
            )
    except Exception as e:
        print("Error reading from Kafka", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(-1)
        
    try:
            parsedLines = lines\
            .selectExpr("CAST(value AS STRING) as json")\
            .select(from_json(col("json"), jsonSchema).alias("parsed"))\
            .select("parsed.*")
    except Exception as e:
        print("Error parsing JSON", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(-1)
    
    client = MongoClient("mongodb://localhost:27017")
    db = client["BigData"]
    collection = db["processed"]
    collection.delete_many({})
    collection = db["data"]
    collection.delete_many({})

    parsedLines = parsedLines.withColumn("time", to_timestamp(col("time"), "d/M/y H:m:s"))

    try:
            mongoLines = (parsedLines\
                .write\
                .format("mongodb")\
                .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")\
                .option("spark.mongodb.write.database", "BigData")\
                .option("spark.mongodb.write.collection", "data")\
                .mode("append")\
                .save()
            )
    except Exception as e:
        print("Error writing to MongoDB", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(-1)

    try: 
            aggregatedData = parsedLines\
                .groupBy("link")\
                .agg(
                    count("name").alias("vcount"),
                    sum(abs(col("speed"))).alias("total_speed")

                )\
                .withColumn("time", F.current_timestamp())
    except Exception as e:
        print("Error aggregating data", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(-1)
                    

    try:
            resultData = aggregatedData\
                .withColumn("vspeed", F.round(col("total_speed") / col("vcount"), 2))\
                .drop("total_speed")
    except Exception as e:
        print("Error calculating average speed", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(-1)
        

    console_query = resultData\
        .write\
        .mode("append")\
        .format("console")\
        .save()
        #.awaitTermination()

    query = resultData.write\
        .format("mongodb")\
        .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")\
        .option("spark.mongodb.write.database", "BigData")\
        .option("spark.mongodb.write.collection", "processed")\
        .mode("append")\
        .save()

        


