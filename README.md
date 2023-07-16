# Pinterest Data Collection Pipeline

This project implements a data collection pipeline for gathering data from Pinterest using FastAPI for user emulation. The collected data is then moved to a Kafka topic for further processing. Additionally, the pipeline supports batch collection, where data is extracted from the Kafka topic and stored in an AWS S3 bucket using Spark. For orchestration of the batch process, Airflow is utilized. The pipeline also includes a streaming process, where Spark is employed to gather and clean data, and then load it directly into a PostgreSQL database.

## Architecture

The Pinterest Data Collection Pipeline consists of several components working together:

- **FastAPI**: The FastAPI framework is used to emulate user interactions with Pinterest. It provides an API endpoint `/pin/` that accepts POST requests and collects data from the Pinterest platform. The received data is then serialized and sent to the Kafka topic `pinterest_topic` using a Kafka producer.

- **Kafka**: The Kafka topic `pinterest_topic` acts as a buffer between the data collection (FastAPI) and further processing steps. It ensures reliable and scalable data transfer, allowing for decoupling of data collection and processing.

- **Batch Processing (Spark and AWS S3)**: The batch processing step involves reading data from the Kafka topic `pinterest_topic` using a Kafka consumer. Each message is then uploaded to an AWS S3 bucket (`pinterest-data-26b723b8-a429-4cd5-9afb-a032ed2dd8c7`) as a separate JSON file. This process is performed using the `KafkaConsumer` and `boto3` libraries. The batch processing step is triggered by an Airflow DAG (`s3_spark_dag`) that schedules and executes the Spark job to retrieve data from Kafka and upload it to AWS S3.

- **Streaming Processing (Spark and PostgreSQL)**: The streaming processing step uses Spark to read data from the Kafka topic `pinterest_topic` in real-time. Each message is parsed, transformed, and cleaned, and then loaded into a PostgreSQL database (`pinterest_streaming`), specifically the table `public.experimental_data`. The streaming processing step is implemented using Spark Structured Streaming and the PostgreSQL JDBC connector.

## Setup and Configuration

To set up the Pinterest Data Collection Pipeline, follow these steps:

1. Clone the project repository to your local machine.

2. Install the required dependencies by running `pip install -r requirements.txt`.

3. Configure the necessary settings in the configuration files:

   - `config/fastapi_config.py`: Set the appropriate Pinterest API credentials and any other relevant configurations for FastAPI.

   - `config/kafka_config.py`: Specify the Kafka broker and topic information.

   - `config/spark_config.py`: Configure the Spark cluster connection details and any other relevant Spark settings.

   - `config/airflow_config.py`: Set the Airflow scheduler and executor configurations.

   - `config/postgresql_config.py`: Provide the connection details for the PostgreSQL database.

4. Start the FastAPI server by running `uvicorn main:app --reload` in the project root directory.

5. Configure Airflow by running `airflow initdb` to initialize the Airflow database.

6. Define your batch processing workflow using Airflow's DAG (Directed Acyclic Graph) definition. Place your DAG file in the `dags/` directory.

7. Start the Airflow scheduler and web server using `airflow scheduler` and `airflow webserver`, respectively.

8. Implement the necessary Spark code to read data from the Kafka topic and perform the required transformations for both batch processing (writing to AWS S3) and streaming processing (writing to PostgreSQL).

9. Run the Airflow DAG to execute the batch processing workflow according to your defined schedule.

10. Verify that the processed data is successfully stored in the AWS S3 bucket and the PostgreSQL database.