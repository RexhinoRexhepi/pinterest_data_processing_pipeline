import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf, from_json, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from dotenv import load_dotenv
import pyspark.sql.functions as F

# Load environment variables from .env file
load_dotenv()

# Initialize Spark
findspark.init()
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.1 pyspark-shell'
kafka_topic_name = "pinterest_topic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Register the follower_count_udf as a UDF
def parse_follower_count(x):
    try:
        if x and x[-1] == "k":
            return int(float(x[:-1]) * 1000)
        elif x and x[-1] in ["m", "M"]:
            return int(float(x[:-1]) * 1000000)
        elif x and x[-1] in ["b", "B"]:
            return int(float(x[:-1]) * 1000000000)
        elif x:
            return int(x)
        else:
            return None
    except ValueError:
        return None

follower_count_udf = udf(parse_follower_count, IntegerType())
spark.udf.register("follower_count_udf", follower_count_udf)

# Read from Kafka topic and save as JSON files locally
stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column to string
stream_df = stream_df.withColumn("value", expr("CAST(value AS STRING)"))

# Define the schema for parsing the JSON
schema = StructType([
    StructField("category", StringType()),
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType())
])

# Parse the JSON string into structured columns
parsed_df = stream_df.withColumn("parsed_value", from_json("value", schema))

# Extract necessary fields and transform the dataframe
transformed_df = parsed_df.selectExpr(
    "current_timestamp() as timestamp",  # Add timestamp column
    "parsed_value.unique_id as unique_id",
    "parsed_value.category as category",
    "parsed_value.index as index_number",
    "parsed_value.title as title",
    "parsed_value.description as description",
    "follower_count_udf(parsed_value.follower_count) as follower_count",
    "parsed_value.tag_list as tag_list",
    "parsed_value.is_image_or_video as is_image_or_video",
    "parsed_value.image_src as image_src",
    "parsed_value.downloaded as downloaded",
    "parsed_value.save_location as save_location"
)

# Define the function to write each batch of data to PostgreSQL using JDBC
def write_to_postgresql(df, epoch_id):
    # Write the DataFrame to PostgreSQL using JDBC
    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql://localhost:5432/pinterest_streaming') \
        .option('dbtable', 'public.experimental_data') \
        .option('user', os.environ["PGADMIN_USERNAME"] ) \
        .option('password', os.environ["PGADMIN_PASS"]) \
        .option('driver', 'org.postgresql.Driver') \
        .save()

# Write each batch of data to PostgreSQL
query = transformed_df \
    .writeStream \
    .foreachBatch(write_to_postgresql) \
    .start()

# Wait for the termination signal
query.awaitTermination()