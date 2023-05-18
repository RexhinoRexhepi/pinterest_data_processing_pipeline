import re
import os
import findspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, split, udf, regexp_replace
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.streaming import StreamingContext
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Spark
findspark.init()
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.6.0 pyspark-shell'
kafka_topic_name = "pinterest_topic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("timestamp", TimestampType(), True), # Add this line for the 'timestamp' column
    StructField("index", StringType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])


# Read from Kafka topic with the specified schema
stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Define the cleaning transformations
follower_count_udf = udf(lambda x: int(float(x[:-1]) * 1000) if x and x[-1] == "k" else
                          int(float(x[:-1]) * 1000000) if x and x[-1] in ["m", "M"] else
                          int(float(x[:-1]) * 1000000000) if x and x[-1] in ["b", "B"] else
                          int(x) if x else None, IntegerType())

# Define the cleaning transformations
cleaned_df = stream_df.select(
    col("index").alias("index"),
    col("unique_id").alias("unique_id"),
    col("title").alias("title"),
    col("description").alias("description"),
    col("poster_name").alias("poster_name"),
    follower_count_udf(regexp_replace(col("follower_count"), "[^\\d.kmB]", "")).alias("follower_count"),
    col("tag_list").alias("tag_list"),
    col("is_image_or_video").alias("is_image_or_video"),
    col("image_src").alias("image_src"),
    col("downloaded").alias("downloaded"),
    col("save_location").alias("save_location"),
    col("category").alias("category")
)

# Write the Kafka message value to a file
stream_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "/home/rexhino/Desktop/pinterest_streaming") \
    .option("checkpointLocation", "/home/rexhino/Desktop/pinterest_streaming") \
    .start() \
    .awaitTermination()

# Print the Kafka message value to check its structure
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

stream_df.printSchema()

# Stream monitoring and saving function
runningCount = 0

def foreach_batch_function(df, epoch_id):
    global runningCount

    if 'timestamp' in df.columns:
        windowed_df = df.withWatermark("timestamp", "2 minutes") \
            .groupBy(F.window("timestamp", "2 minutes", "1 minutes"))
        # Perform further operations on windowed_df if required
    else:
        print("The 'timestamp' column is not present in the DataFrame.")
        return

    # Group by sliding window
    windowed_df = windowed_df.groupBy(
        F.window("timestamp", "2 minutes", "1 minutes"),
        df.is_image_or_video
    ).count()

    # Cumulative count of null data in the stream so far
    if windowed_df.first()[1]:
        runningCount += windowed_df.first()[2]
        print("Errors so far:", runningCount)

    windowed_df.show(truncate=False)

    # Send processed stream data to PostgreSQL
    windowed_df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5050/server1/pinterest_streaming') \
        .option('user', os.environ["PGADMIN_USERNAME"]) \
        .option('password', os.environ["PGADMIN_PASS"]) \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'pinterest_streaming') \
        .save()

# Send processed stream data to PostgreSQL
query = cleaned_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()

query.awaitTermination()