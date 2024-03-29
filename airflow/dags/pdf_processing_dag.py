import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import csv,os,sys

def pdf_extraction(s3):
    print("PDF extraction script running...")
    # Path to the PDF extraction script
    script_path = "../Scripts/PDFExtraction.py"
    
    try:
        # Run the PDF extraction script using subprocess
        subprocess.run(["python", script_path] + s3, check=True)
        print("PDF extraction completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: PDF extraction script returned non-zero exit status {e.returncode}")

 
def webscraping():
    print("Web Scraping script running...")
    script_path = "../Scripts/Webscraping.py"
    
    try:
        subprocess.run(["python", script_path], check=True)
        print("Web Scraping completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Web Scraping script returned non-zero exit status {e.returncode}")
 
def data_validation():
    print("Data Validation script running...")
    script_path = "../Scripts/DataValidation.py"
    
    try:
        subprocess.run(["python", script_path], check=True)
        print("Data Validation completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Data Validation script returned non-zero exit status {e.returncode}")
 
def sql_alchemy():
    print("Snowflake Data Upload script running...")
    script_path = "../Scripts/SQLAlchemy.py"
    
    try:
        subprocess.run(["python", script_path], check=True)
        print("Snowflake Data Upload completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Snowflake Data Upload script returned non-zero exit status {e.returncode}")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
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
    python_callable=webscraping,
    dag=dag,
)

task_pdf_extraction = PythonOperator(
    task_id='PDF_Extraction',
    python_callable=pdf_extraction,
    op_kwargs={'s3_locations': '{{ dag_run.conf["s3_locations"] }}'},  # Get list of S3 locations from DAG run
    dag=dag,
)

task_data_validation = PythonOperator(
    task_id='Data_Validation',
    python_callable=data_validation,
    dag=dag,
)

task_snowflake_sqlalchemy = PythonOperator(
    task_id='Upload_to_Snowflake',
    python_callable=sql_alchemy,
    dag=dag,
)

# Set dependencies
[task_pdf_extraction, task_webscraping] >> task_data_validation >> task_snowflake_sqlalchemy
