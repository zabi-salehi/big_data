from typing import ValuesView
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructType, TimestampType

import mysql.connector
# from mysql.connector import errorcode

# dbOptions = {"host": "my-app-mariadb-service", 'port': 33060,
#              "user": "root", "password": "mysecretpw",
#              "schema": "popular"}
# dbSchema = 'popular'
windowDuration = '5 minutes'
slidingDuration = '1 minute'

# Example Part 1
# Create a spark session
spark = SparkSession.builder \
    .appName("Structured Streaming").getOrCreate()

# Set log level
spark.sparkContext.setLogLevel('WARN')

# Example Part 2
# Read messages from Kafka
kafkaMessages = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "my-cluster-kafka-bootstrap:9092") \
    .option("subscribe", "tracking-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Define schema of tracking data
trackingMessageSchema = StructType() \
    .add("show_id", StringType()) \
    .add("title", StringType()) \
    .add("director", StringType()) \
    .add("cast", StringType()) \
    .add("country", StringType()) \
    .add("release_year", IntegerType()) \
    .add("duration", StringType()) \
    .add("genre", StringType()) \
    .add("description", StringType()) \
    .add("timestamp", IntegerType()) # @ missing cached?

# Example Part 3
# Convert value: binary -> JSON -> fields + parsed timestamp
trackingMessages = kafkaMessages.select(
    # Extract 'value' from Kafka message (i.e., the tracking data)
    from_json(
        column("value").cast("string"),
        trackingMessageSchema
    ).alias("json")
).select(
    # # Convert Unix timestamp to TimestampType
    from_unixtime(column('json.timestamp'))
    .cast(TimestampType())
    .alias("parsed_timestamp"),

    # Select all JSON fields
    column("json.*")
) \
    .withColumnRenamed('json.show_id', 'title') \
    .withColumnRenamed('json.title', 'title') \
    .withColumnRenamed('json.director', 'director') \
    .withColumnRenamed('json.cast', 'cast') \
    .withColumnRenamed('json.country', 'country') \
    .withColumnRenamed('json.release_year', 'release_year') \
    .withColumnRenamed('json.duration', 'duration') \
    .withColumnRenamed('json.genre', 'genre') \
    .withColumnRenamed('json.description', 'description') \
    .withWatermark("parsed_timestamp", windowDuration)

# Example Part 4

# Compute views of titles
views_titles = trackingMessages.groupBy(
    window(
        column("parsed_timestamp"),
        windowDuration,
        slidingDuration
    ),
    column("show_id")
).count().withColumnRenamed('count', 'views')
# views_titles.views *= 5 # a view is worth more than other scores


# Compute views of titles for their directors
tm1 = trackingMessages.alias("tm1")
tm2 = trackingMessages.alias("tm2")

title_director_mapping = tm1.join(
    tm2,
    tm1.director == tm2.director,
    "inner"
).groupBy(
    window(
        column("tm1.parsed_timestamp"),
        windowDuration,
        slidingDuration
    ),
    column("tm2.show_id")
).count().withColumnRenamed('count', 'views')


# Join final rating
# final_rating = views_titles


# Example Part 5
# Start running the query; print running counts to the console
consoleDump = views_titles \
    .writeStream \
    .trigger(processingTime=slidingDuration) \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()



# Example Part 6

def saveToDatabase(batchDataframe, batchId):
    # Define function to save a dataframe to mysql
    def save_to_db(iterator):
        # Connect to database and use schema
        # session = mysqlx.get_session(dbOptions)
        # session = mysqlx.get_session("mysqlx://root:mysecretpw@my-app-mariadb-service:33060/popular")


        session = mysql.connector.connect(user='root', password='mysecretpw', host='my-app-mariadb-service', database='netflix_titles')
        cursor = session.cursor()

        for row in iterator:
            query = f"INSERT INTO rating (show_id, rating) VALUES ('{row.show_id}', {row.views}) ON DUPLICATE KEY UPDATE rating={row.views};"
            cursor.execute(query)
                            

        session.commit()
        cursor.close()
        session.close()


    # Perform batch UPSERTS per data partition
    batchDataframe.foreachPartition(save_to_db)

# Example Part 7
dbInsertStream = views_titles.writeStream \
    .trigger(processingTime=slidingDuration) \
    .outputMode("update") \
    .foreachBatch(saveToDatabase) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
