# Pinterest Data Pipeline

This project implements an end-to-end data pipeline to ingest and process Pinterest data. It contains the following components:

- API to collect Pinterest post data
- Kafka producer to publish messages
- Kafka consumer to read messages
- Upload JSON files to S3
- Spark job to process data from S3
- Airflow DAG to orchestrate Spark job
- Spark Structured Streaming from Kafka to PostgreSQL

## API

The FastAPI collects Pinterest post data and publishes to a Kafka topic:

```python
@app.post("/pin/")
def get_db_row(item: Data):

  data = dict(item)
 
  pinterest_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="pinterest_pipeline",
    value_serializer=lambda m: dumps(m).encode("ascii")
  )

  pinterest_producer.send(topic="pinterest_topic", value=data)

  return item
```

The `Data` model defines the schema:

```python
class Data(BaseModel):

  category: str
  index: int
  unique_id: str
  # other attributes
```

## Kafka Producer

The producer publishes messages to Kafka topic `pinterest_topic`:

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
  bootstrap_servers='localhost:9092',
  value_serializer=lambda m: json.dumps(m).encode('utf-8') 
)

producer.send('pinterest_topic', data)
```

## Kafka Consumer

The consumer reads messages from `pinterest_topic` and saves to S3:

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
  'pinterest_topic',
  bootstrap_servers=['localhost:9092'],
  value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

for message in consumer:
 
  # save message to S3
  s3.put_object(Body=json.dumps(message.value), Bucket='mybucket', Key=f'message_{i}.json')
```

## Spark Processing

Spark is used to process the raw JSON files from S3:

```python
df = spark.read.json('s3a://mybucket/*.json')

cleaned_df = transform(df)

cleaned_df.write.parquet('s3a://mybucket/cleaned')
```

The `transform()` function cleans the data.

## Airflow DAG

Airflow orchestrates running the Spark job:

```python
spark_task = BashOperator(
  task_id='spark_task',
  bash_command='spark-submit --packages com.amazonaws:aws-java-sdk-s3:1.12.61,org.apache.hadoop:hadoop-aws:3.3.2 s3_to_spark.py'
)
```

## Spark Streaming to PostgreSQL

Finally, Spark Structured Streaming is used to stream new messages from Kafka to PostgreSQL:

```python
df = spark.readStream.format("kafka").load()

df.writeStream.foreachBatch(write_to_postgres).start()

def write_to_postgres(df, epoch_id):
  df.write.jdbc(url="jdbc:postgresql://localhost/mydb", table="messages")
```

This loads new messages from Kafka and inserts them into a PostgreSQL table.

The full code for this project is available in this repository. The project implements an end-to-end pipeline to collect Pinterest data, process with Spark, and load into PostgreSQL for analysis.