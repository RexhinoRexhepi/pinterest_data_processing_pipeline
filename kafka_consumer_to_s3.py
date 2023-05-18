from kafka import KafkaConsumer
import json
import boto3

# create a Kafka consumer to consume messages from the topic
consumer = KafkaConsumer(
    'pinterest_topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

# create an S3 client to upload the files
s3 = boto3.client('s3')

# consume messages from Kafka and upload them to S3
for i, message in enumerate(consumer):
    data = {"PinterestTopic": message.topic, "Message": message.value}
    filename = f"user_post_{i + 1}.json"
    s3.put_object(Body=json.dumps(data), Bucket='pinterest-data-26b723b8-a429-4cd5-9afb-a032ed2dd8c7', Key=filename)