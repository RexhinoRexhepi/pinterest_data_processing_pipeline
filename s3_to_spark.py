from pyspark.sql import functions as F
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import regexp_replace, col, udf
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

os.environ["SPARK_LOCAL_IP"] = "192.168.0.13"

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.61,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Creating our Spark configuration
conf = SparkConf()\
        .setAppName('S3toSpark')\
        .setMaster('local[*]')
        
sc = SparkContext(conf=conf)

# Create our Spark session
spark = SparkSession(sc).builder\
        .appName("S3App")\
        .config("spark.executor.userClassPathFirst", "true") \
        .config("spark.driver.userClassPathFirst", "true") \
        .getOrCreate()

# Configure the setting to read from the S3 bucket
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS
hadoopConf.set('spark.hadoop.fs.s3a.endpoint', 's3.eu-west-2.amazonaws.com') # Set the S3 endpoint to eu-west-2

# Define the S3 bucket and directory containing the JSON files
s3_bucket = "pinterest-data-26b723b8-a429-4cd5-9afb-a032ed2dd8c7"

# Read the JSON files from the S3 bucket into a Spark dataframe

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .json(f"s3a://{s3_bucket}/*.json")

# Define the cleaning transformations
follower_count_udf = udf(lambda x: int(float(x[:-1]) * 1000) if x and x[-1] == "k" else
                          int(float(x[:-1]) * 1000000) if x and x[-1] in ["m", "M"] else
                          int(float(x[:-1]) * 1000000000) if x and x[-1] in ["b", "B"] else
                          int(x) if x else None, IntegerType())


# Define the cleaning transformations
cleaned_df = (df.select(col("Message.unique_id").alias("unique_id"),
                        col("Message.category").alias("category"),
                        col("Message.index").alias("index"),
                        col("Message.title").alias("title"),
                        col("Message.description").alias("description"),
                        follower_count_udf(regexp_replace(col("Message.follower_count"), "[^\\d.kmB]", "")).alias("follower_count"),
                        col("Message.tag_list").alias("tag_list"),
                        col("Message.is_image_or_video").alias("is_image_or_video"),
                        col("Message.image_src").alias("image_src"),
                        col("Message.downloaded").alias("downloaded"),
                        col("Message.save_location").alias("save_location")))



# Write the cleaned data to a new file
cleaned_df.coalesce(1).write.mode("overwrite").json("/home/rexhino/Web_Scraping_Project/Pinterest_Data_Processing_Pipeline/Pinterest_App/API/cleaned_data.json")

# Show the data
cleaned_df.show()

# Stop the SparkSession
spark.stop()