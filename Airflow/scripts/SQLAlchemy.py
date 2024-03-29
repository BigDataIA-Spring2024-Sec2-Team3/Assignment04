#!/usr/bin/env python
import warnings
warnings.filterwarnings("ignore")
from dotenv import load_dotenv
from sqlalchemy import create_engine
import bcrypt
import os
import csv

def convert_to_url(column_data, bucket_name):
    # Constructing the URL based on the first column data
    trimmed_data = column_data[5:7]
    url = f"https://s3.console.aws.amazon.com/s3/object/{bucket_name}?region=us-east-1&bucketType=general&prefix=Grobid_RR_2024_{trimmed_data}_combined.txt"
    return url

load_dotenv()

u=os.getenv("BUCKET_NAME")

# Open the CSV file for reading and writing
with open('../../data/metadata.csv', mode='r') as file:
    reader = csv.DictReader(file)
    fieldnames = reader.fieldnames + ['URL']
    
    # Create a new CSV file with an additional column for the URLs
    with open('../../data/metadata_new.csv', mode='w', newline='') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        
        # Iterate over each row in the CSV file
        for row in reader:
            column_data = row['filename']  # Assuming the first column contains the data for conversion
            url = convert_to_url(column_data, u)
            row['URL'] = url
            
            # Write the modified row to the new CSV file
            writer.writerow(row)
    
    print("Conversion completed. Output saved to output.csv")


# create_test_stage = """CREATE STAGE TEST_PDF_STAGING DIRECTORY = ( ENABLE = true );"""
    
# create_test_table = """CREATE OR REPLACE TABLE test_cfa_courses (
#         NameOfTopic STRING,
#         Title STRING,
#         Year INTEGER,
#         Level STRING,
#         Introduction STRING,
#         LearningOutcome STRING,
#         LinkToPDF STRING,
#         LinkToSummary STRING
#         );"""

# create_test_contents_table = """CREATE OR REPLACE TABLE test_pdf_contents (
#         ContentID INTEGER,
#         DocID INTEGER,
#         Level STRING,
#         Year INTEGER,
#         Title STRING,
#         Article STRING,
#         LearningOutcome STRING
#         );"""

# create_test_metadata_table = """CREATE OR REPLACE TABLE test_pdf_metadata (
#         DocID INTEGER,
#         Filename STRING,
#         Title STRING,
#         IDNo STRING,
#         Level STRING,
#         Year INTEGER,
#         TextLink STRING
#         );"""
    
# copy_stage_to_test_table = """COPY INTO test_cfa_courses
#         FROM @PC_DBT_DB.public.TEST_URL_STAGING
#         FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' skip_header = 1)
#         PATTERN = 'Validation_data.csv.gz'
#         ON_ERROR = 'skip_file';"""

# upload_contents_to_test_stage = """PUT file://..\..\data\content.csv @PC_DBT_DB.public.TEST_PDF_STAGING;"""
# upload_metadata_to_test_stage = """PUT file://..\..\data\metadata_new.csv @PC_DBT_DB.public.TEST_PDF_STAGING;"""
# upload_to_test_stage = """PUT file://../..\data\Validation_data.csv @PC_DBT_DB.public.TEST_URL_STAGING;"""

# copy_contents_to_test_table = """COPY INTO test_pdf_contents
#   FROM @PC_DBT_DB.public.TEST_PDF_STAGING
#   FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' skip_header = 1)
#   PATTERN = 'content.csv.gz'
#   ON_ERROR = 'skip_file';"""

# copy_metadata_to_test_table = """COPY INTO test_cfa_courses
#   FROM @PC_DBT_DB.public.TEST_PDF_STAGING
#   FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' skip_header = 1)
#   PATTERN = 'metadata_new.csv.gz'
#   ON_ERROR = 'skip_file';"""


create_prod_stage = """CREATE STAGE PDF_STAGING DIRECTORY = ( ENABLE = true );"""

create_prod_content_table_query = """CREATE OR REPLACE TABLE pdf_contents (
        ContentID INTEGER,
        DocID INTEGER,
        Level STRING,
        Year INTEGER,
        Title STRING,
        Article STRING,
        LearningOutcome STRING
        );"""

create_prod_table_query = """CREATE OR REPLACE TABLE cfa_courses (
        NameOfTopic STRING,
        Title STRING,
        Year INTEGER,
        Level STRING,
        Introduction STRING,
        LearningOutcome STRING,
        LinkToPDF STRING,
        LinkToSummary STRING
        );"""

create_prod_metadata_table = """CREATE OR REPLACE TABLE pdf_metadata (
        DocID INTEGER,
        Filename STRING,
        Title STRING,
        IDNo STRING,
        Level STRING,
        Year INTEGER,
        TextLink STRING
        );"""

upload_to_prod_stage = """PUT file://../..\data\Validation_data.csv @PC_DBT_DB.public.URL_STAGING;"""

upload_contents_to_prod_stage = """PUT file://..\..\data\content.csv @PC_DBT_DB.public.PDF_STAGING;"""

upload_metadata_to_prod_stage = """PUT file://..\..\data\metadata_new.csv @PC_DBT_DB.public.PDF_STAGING;"""

copy_stage_content_to_prod_table = """COPY INTO pdf_contents
        FROM @PC_DBT_DB.public.PDF_STAGING
        FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' skip_header = 1)
        PATTERN = 'content.csv.gz'
        ON_ERROR = 'skip_file';"""

copy_metadata_to_prod_table = """COPY INTO pdf_metadata
        FROM @PC_DBT_DB.public.PDF_STAGING
        FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' skip_header = 1)
        PATTERN = 'metadata_new.csv.gz'
        ON_ERROR = 'skip_file';"""

copy_stage_to_prod_table = """COPY INTO cfa_courses
        FROM @PC_DBT_DB.public.URL_STAGING
        FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' skip_header = 1)
        PATTERN = 'Validation_data.csv.gz'
        ON_ERROR = 'skip_file';"""

load_dotenv()

u=os.getenv("SNOWFLAKE_USER")
p=os.getenv("SNOWFLAKE_PASS")
ai=os.getenv("SNOWFLAKE_ACCOUNTID")


engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user=u,
        password=p,
        account_identifier=ai,
    )
)



try:
    connection = engine.connect()
    connection.execute("USE DATABASE PC_DBT_DB")
    connection.execute("USE WAREHOUSE PC_DBT_WH")
    
    #results = connection.execute(create_test_stage)
    # results = connection.execute(create_test_contents_table)
    # results = connection.execute(create_test_metadata_table)
    # results = connection.execute(upload_contents_to_test_stage)
    # results = connection.execute(upload_metadata_to_test_stage)
    # results = connection.execute(copy_contents_to_test_table)
    # results = connection.execute(copy_metadata_to_test_table)
    # results = connection.execute(create_test_table)
    # results = connection.execute(upload_to_test_stage)
    # results = connection.execute(copy_stage_to_test_table)
    

    results = connection.execute(create_prod_stage)
    results = connection.execute(create_prod_content_table_query)
    results = connection.execute(create_prod_metadata_table)
    results = connection.execute(upload_contents_to_prod_stage)
    results = connection.execute(upload_metadata_to_prod_stage)
    results = connection.execute(copy_stage_content_to_prod_table)
    results = connection.execute(copy_metadata_to_prod_table)
    results = connection.execute(create_prod_table_query)
    results = connection.execute(upload_to_prod_stage)
    results = connection.execute(copy_stage_to_prod_table)


finally:
    print("Done")
    connection.close()
    engine.dispose()