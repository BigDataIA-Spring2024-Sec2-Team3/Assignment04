from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from lxml import etree
from bs4 import BeautifulSoup
import csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
}

dag = DAG(
    'pdf_processing_dag',
    default_args=default_args,
    description='DAG for processing PDF files with GROBID and BeautifulSoup',
    schedule_interval=None,
)

def process_pdf_files(s3_locations):
    for pdf_url in s3_locations:
        # Fetch PDF content from the provided S3 URL
        response = requests.get(pdf_url)
        pdf_content = response.content
        
        # Process PDF content with GROBID
        grobid_url = 'http://localhost:8070/api/processFulltextDocument'
        files = {'input': pdf_content}
        grobid_response = requests.post(grobid_url, files=files)
        
        # Parse GROBID XML response
        xml_content = grobid_response.content
        root = etree.fromstring(xml_content)
        
        # Extract metadata from GROBID XML
        # Modify this part according to your XML structure
        metadata = {
            'Title': root.xpath('//tei:title[@type="main"]/text()')[0],
            'Author': root.xpath('//tei:author/text()')[0],
            # Add more metadata fields as needed
        }
        
        # Extract text content from GROBID XML
        text_content = root.xpath('//tei:text//text()')
        parsed_text = ' '.join(text_content)
        
        # Process text content with BeautifulSoup (example)
        soup = BeautifulSoup(parsed_text, 'html.parser')
        paragraphs = [p.get_text() for p in soup.find_all('p')]
        
        # Save metadata to CSV file
        metadata_csv_filename = '/tmp/metadata.csv'
        with open(metadata_csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=metadata.keys())
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(metadata)
        
        # Save text content to CSV file
        content_csv_filename = '/tmp/content.csv'
        with open(content_csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for paragraph in paragraphs:
                writer.writerow([paragraph])

# Define PythonOperator to execute the PDF processing function
process_pdf_task = PythonOperator(
    task_id='process_pdf_files',
    python_callable=process_pdf_files,
    op_kwargs={'s3_locations': '{{ dag_run.conf["s3_locations"] }}'},  # Get list of S3 locations from DAG run
    dag=dag,
)

process_pdf_task