import os
from dotenv import load_dotenv
import logging
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from plugins.download_pdf import download_pdf
from plugins.parse_xml import parse_xml 
from plugins.grobid_parsing import PDF_XML_function
from plugins.snowflake_code import snowflake_upload
load_dotenv(override=True)

dag = DAG(
    dag_id="handle_pdf_dag",
    schedule_interval="0 0 * * *",  # Daily at midnight
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["pdf_processing", "s3", "snowflake"],
)

def start_message():
    print("Starting PDF processing task")

def end_message():
    print("PDF processing task completed")

def delete_all_files():
    folder_path = os.getenv('AIRFLOW_FILES_PATH')
    if folder_path is not None:
        for root, dirs, files in os.walk(folder_path):
            if files is None:
                return None
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Deleted: {file_path}")
    else:
        logging.error('No Path for files')


with dag:
    start_task = PythonOperator(
        task_id='start_message',
        python_callable=start_message,
    )

    download_pdf_task = PythonOperator(
        task_id='download_pdf',
        python_callable=download_pdf,
        op_kwargs={
            'bucket_name': "{{ dag_run.conf.get('bucket_name') }}",
            'file_keys': "{{ dag_run.conf.get('file_keys') }}"
        },
    )

    grobid_parsing_task = PythonOperator(
        task_id='grobid_pdf_to_xml',
        python_callable=PDF_XML_function,
        op_kwargs={
            'pdf_files': "{{ dag_run.conf.get('file_keys') }}"
        },
    )

    parse_xml_to_csv = PythonOperator(
        task_id='parse_xml_to_csv',
        python_callable=parse_xml,
        op_kwargs={
            'user_id': "{{ dag_run.conf.get('user_id') }}"
        },
    )

    files_upload = PythonOperator(
        task_id='snowflake_upload',
        python_callable=snowflake_upload,
    )

    files_delete = PythonOperator(
        task_id='delete_files',
        python_callable=delete_all_files,
    )

    end_task = PythonOperator(
        task_id='end_message',
        python_callable=end_message,
    )

    start_task >> download_pdf_task >> grobid_parsing_task >> parse_xml_to_csv >> files_upload >> files_delete >> end_task #type: ignore