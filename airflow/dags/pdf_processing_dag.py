from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
from lxml import etree
from bs4 import BeautifulSoup
import csv

from Python_Scripts import DataValidation, PDFParsing, SQLAlchemy, Webscraping

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'PDF_Processing_DAG',
    default_args=default_args,
    description='DAG for processing PDF files with GROBID and BeautifulSoup',
    schedule_interval=None,
)

task_webscraping = PythonOperator(
    task_id='Webscraping',
    python_callable=Webscraping.main,
    dag=dag,
)

task_data_validation = PythonOperator(
    task_id='Data_Validation',
    python_callable=DataValidation.main,
    dag=dag,
)

# Define PythonOperator to execute the PDF processing function
task_pdf_extraction = PythonOperator(
    task_id='PDF_Extraction',
    python_callable=PDFParsing.pdf_parsing,
    op_kwargs={'s3_locations': '{{ dag_run.conf["s3_locations"] }}'},  # Get list of S3 locations from DAG run
    dag=dag,
)

task_snowflake_sqlalchemy = PythonOperator(
    task_id='Upload_to_Snowflake',
    python_callable=SQLAlchemy.main,
    dag=dag,
)

# Set dependencies
[task_pdf_extraction, task_webscraping] >> task_data_validation >> task_snowflake_sqlalchemy
