# Pinterest Data Pipeline

This project implements a data pipeline to process and analyze data from Pinterest. It consists of multiple components, including a FastAPI server, a Kafka producer, a Kafka consumer, an S3 uploader, and Spark data processing.

## Architecture

The architecture of the Pinterest Data Pipeline is as follows:

The pipeline follows the following steps:

1. The FastAPI server receives data in JSON format via a POST request.
2. The received data is sent to a Kafka topic by the Kafka producer.
3. The Kafka consumer reads data from the Kafka topic.
4. The consumer uploads the data to an S3 bucket.
5. The Spark data processing component retrieves data from the S3 bucket and performs data processing and analysis.

## Components

### FastAPI Server

The FastAPI server is responsible for receiving data from clients. It exposes an endpoint `/pin/` that accepts a JSON payload containing Pinterest data. The server validates the data against a defined data model and sends the data to a Kafka topic.

To start the FastAPI server, run the following command:

```shell
uvicorn project_pin_API:app --host=localhost --port=8000
```
## Kafka Producer

The Kafka producer receives data from the FastAPI server and sends it to a Kafka topic. It uses the KafkaProducer class from the `kafka` library to establish a connection with the Kafka broker and send messages.

## Kafka Consumer

The Kafka consumer reads data from the Kafka topic. It retrieves messages using the KafkaConsumer class from the `kafka` library and deserializes the data. In this pipeline, the consumer uploads the data to an S3 bucket.

## S3 Uploader

The S3 uploader component is responsible for uploading data received from the Kafka consumer to an S3 bucket. It uses the `boto3` library to interact with Amazon S3. The uploaded data is stored in the specified S3 bucket for further processing.

## Spark Data Processing

The Spark data processing component reads data from the S3 bucket and performs data processing and analysis. It uses Apache Spark's `SparkSession` to create a Spark context and read data from the S3 bucket. The processed data can be used for various analytics tasks.

## Configuration

Before running the Pinterest Data Pipeline, make sure to set up the necessary configurations:

- Install the required dependencies by running `pip install -r requirements.txt`.
- Set the Kafka broker address and S3 bucket details in the appropriate components.
- Configure the AWS access key and secret access key for S3 access.

## Contributing

Contributions to the Pinterest Data Pipeline project are welcome! If you find any issues or want to add new features, feel free to open a pull request.