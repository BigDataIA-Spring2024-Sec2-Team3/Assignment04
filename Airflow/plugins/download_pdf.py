# Filename: download_pdf.py (to be placed in the Airflow plugins folder)
import boto3
from dotenv import load_dotenv
import os
from io import BytesIO
from PyPDF2 import PdfReader
import json
load_dotenv(override=True)


def download_pdf(bucket_name, file_keys):
    download_path = os.getenv('AIRFLOW_FILES_PATH')
    # download_path = '/Users/ldy/git/dokcer_airflow/Airflow/files'
    ak = os.getenv("AWS_SK")
    aki = os.getenv("AWS_AK")
    if download_path is not None:
        if not os.path.exists(download_path):
            os.makedirs(download_path)
    else:
        print('No file path in the docke image')
        exit(1)

    s3_client = boto3.client('s3',aws_access_key_id = aki, aws_secret_access_key =ak)
    

    file_keys_str = file_keys.replace("'", '"')

    print('file_keys_str:', file_keys_str)

    file_keys_list = []
    try:
        file_keys_list = json.loads(file_keys_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e} {file_keys_list}")
    else:
        print('replaced list: ',file_keys_list)  # Output will be a list: ['CaseStudyOnAgriculture.pdf']



    for file_key in file_keys_list:
        print('file_keys:----------', file_keys)
        print(type(file_keys))   
        print(f'file_key is:   {file_key}')     
        local_filename = os.path.join(download_path, file_key)
        print('local_filepath-------------:', local_filename)

        s3_client.download_file(bucket_name, file_key, local_filename)

# download_pdf( os.getenv('BUCKET_NAME'), ['2024-l1-topics-combined-2.pdf'])
