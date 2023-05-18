from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the DAG
default_args = {
    'owner': 'Rexhino',
    'email': ['rexhino.rexhepi@outlook.it'],
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='s3_spark_dag',
         description='DAG to run Spark job to get files from S3 bucket',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['test']
         ) as dag:

    # Define the BashOperator to trigger the Spark job
    get_files = BashOperator(
        task_id='get_files_from_s3',
        bash_command='spark-submit --packages com.amazonaws:aws-java-sdk-s3:1.12.61,org.apache.hadoop:hadoop-aws:3.3.2 /home/rexhino/Web_Scraping_Project/Pinterest_Data_Processing_Pipeline/Pinterest_App/API/s3_to_spark.py',
        dag=dag
    )
