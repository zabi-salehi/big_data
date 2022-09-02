from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import IntegerType, StringType, StructType, TimestampType

import mysql.connector

windowDuration = '10 minutes'
slidingDuration = '30 seconds'


# Create a spark session
spark = SparkSession.builder \
    .appName("Structured Streaming").getOrCreate()

# Set log level
spark.sparkContext.setLogLevel('WARN')

# Read messages from Kafka with topic "tracking-data"
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
    .add("director", StringType()) \
    .add("timestamp", IntegerType())


# Convert value: binary -> JSON -> fields + parsed timestamp
trackingMessages = kafkaMessages.select(
    # Extract 'value' from Kafka message (the tracking data)
    F.from_json(
        F.column("value").cast("string"),
        trackingMessageSchema
    ).alias("json")
).select(
    # Convert Unix timestamp to TimestampType
    F.from_unixtime(F.column('json.timestamp'))
    .cast(TimestampType())
    .alias("parsed_timestamp"),

    # Select all JSON fields
    F.column("json.*")
).withColumnRenamed('json.show_id', 'show_id') \
    .withColumnRenamed('json.director', 'director') \
    .withWatermark("parsed_timestamp", windowDuration)


# Calculate Rating

# Aggregate views of shows over time windows
views = trackingMessages.groupBy(
    F.window(
        F.col("parsed_timestamp"),
        windowDuration,
        slidingDuration
    ),
    F.col("show_id"),
    F.col("director")
).count().withColumnRenamed("count", "rating")

# Create two aliases for join operation
tm1 = views.withColumnRenamed("rating", "title_views").alias("tm1")
tm2 = views.withColumnRenamed("rating", "director_views").alias("tm2")

# Apply logic to calculate rating of shows
# For each view of a show A, the show gets 5 rating points
# The view of another show B by the same director as show A adds 1 rating point for the first show A and vice versa

# Example: 1 view for show A and 1 view for show B, both have the same director
# Rating of each show A and B = 5 + 1 = 6

# Using left outer join, so shows without a matching director of show B still get the rating for their views

final_rating = views

# The following piece of code wil cause the application to not work
# It is supposed to calculate the score for shows with the same director as described in the documentation
# For a functional version of the application the rating consists only of the views

"""
final_rating = tm1.join(
        tm2,
        [
            F.col("tm1.director") == F.col("tm2.director"), # outer join of shows with same directors
            F.col("tm1.show_id") != F.col("tm2.show_id"),   # but not exactly the same show
            F.col("tm1.window") == F.col("tm2.window")      # in same time window
        ],
        "left_outer"
).select(
    F.col("tm1.director"),
    F.col("tm2.director"),
    F.col("tm1.show_id"),
    F.col("tm1.window"),
    F.col("tm1.title_views"),
    F.col("tm2.director_views")
) \
    .withColumn("director_rating", F.when(F.col("tm2.director").isNotNull(), F.col("tm2.director_views") * 1).otherwise(F.lit(0))) \
    .withColumn("view_rating", F.col("tm1.title_views") * 5) \
    .withColumn("final_rating", F.col("director_rating") + F.col("view_rating")) \
    .groupBy(
        F.col("tm1.window"),
        F.col("tm1.show_id"),
        F.col("final_rating")
    ).sum().withColumnRenamed("final_rating", "rating")
"""


# Print current rating to the console
consoleDump = final_rating \
    .writeStream \
    .trigger(processingTime=slidingDuration) \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# Save calculated rating to database for web application to access
def saveToDatabase(batchDataframe, batchId):
    def save_to_db(iterator):
        session = mysql.connector.connect(user='root', password='mysecretpw', host='my-app-mariadb-service', database='netflix_titles')
        cursor = session.cursor()

        for row in iterator:
            query = f"INSERT INTO rating (show_id, rating) VALUES ('{row.show_id}', {row.rating}) ON DUPLICATE KEY UPDATE rating={row.rating};"
            cursor.execute(query)
                            
        session.commit()
        cursor.close()
        session.close()

    # Perform batch UPSERTS per data partition
    batchDataframe.foreachPartition(save_to_db)


dbInsertStream = final_rating.writeStream \
    .trigger(processingTime=slidingDuration) \
    .outputMode("update") \
    .foreachBatch(saveToDatabase) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
