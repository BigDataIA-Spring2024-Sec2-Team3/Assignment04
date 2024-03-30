# Assignment04

## Problem Statement
Many data-driven projects involve extracting data from various sources, such as CSV and XML files, and transforming it for analysis or storage. However, ensuring the quality and integrity of this data throughout the process can be challenging. Till now, we have made the ELT pipelines for extraction, schema validations and transformations. Now, the goal is to automate the entire process using AirFlow and develop API's with a user interface to give the end user the power to implement it all using single-click operations.

## Project Goals
The aim of this project is to develop a robust web application workflow for processing and extracting data from PDF files. Below is a breakdown of the tasks implemented in the flow to achieve our project objectives:

1. User Interface
Implement a user-friendly interface to handle PDF uploads and user queries.
2. Application Hosting and Containerization
Deploy Google Cloud Engines to host the web application, allowing for scalable processing power.
Utilize Docker to containerize the application, ensuring consistent environments and easy deployment across instances.
3. Automation and Processing Pipeline
Integrate Streamlit to create an interactive web interface for users to upload PDF files directly into the system.
Utilize FastAPI to build efficient and performant RESTful APIs for handling user queries and automating interactions with the processing pipeline.
4. Workflow Execution and Data Management
Implement an automated pipeline, triggered by Airflow, to manage tasks from PDF upload on S3 to deployment on GCP.
Store PDF files securely and manage them effectively using S3.
5. Data Extraction and Validation
Run snowflake_objects.sql file to create objects into snowflake required for the application.
Automate the extraction of data from PDF files using Python scripts.
Validate extracted data with Pydantic to ensure integrity and structure before further processing.
6. Data Loading and Storage
Load the validated data into Snowflake, a cloud data warehouse, for persistent storage, analysis, and reporting.
Ensure that both PDF content and metadata are handled correctly during the loading process.
The successful implementation of these tasks will result in a streamlined process for PDF data management, from the point of user interaction to data storage and analysis. Our workflow is designed to be resilient, scalable, and maintainable, with clear separation of concerns and ease of monitoring.

Technologies Used
Streamlit GitHub FastAPI Amazon AWS Python Pandas Apache Airflow Docker Google Cloud MongoDB Snowflake

Architecture:
Alt text

- [![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=1rR8MdTSyWoAmdOa4enqDqAAH6V3XI_wwgrctNUDVrQQ/edit#4)

Pre-requisites
Before running this project, ensure you have the following prerequisites set up:

Python: Ensure Python is installed on your system.

Docker: Ensure Docker-desktop is installed on your system.

Virtual Environment: Set up a virtual environment to manage dependencies and isolate your project's environment from other Python projects. You can create a virtual environment using virtualenv or venv.

requirements.txt: Install the required Python dependencies by running the command:

pip install -r requirements.txt
Config File: Set up the configurations file with the necessary credentials and configurations.

Snowflake: Use airflow/dags/load/snowflake_objects.sql to define the queries on snowflake. Also, ensure you have the necessary credentials and configurations set up in the configurations file for connecting to Snowflake.

Google Cloud Platform: Create a Google Cloud Engine. Ensure you have the necessary credentials and configurations set up in the configurations.properties file.

Program Structure
. 
├─ .gitignore
├─ README.md
├─ airflow
│  ├─ Dockerfile
│  ├─ dags
│  │  ├─ airflow-pipeline.py
│  │  ├─ extraction
│  │  │  └─ pdf_extraction.py
│  │  ├─ load
│  │  │  ├─ load_data_to_snowflake.py
│  │  │  └─ snowflake_objects.sql
│  │  └─ validation
│  │     └─ corrections
│  │        ├─ correction_pdf_content.py
│  │        ├─ correction_pdf_metadata.py
│  │        └─ utils
│  │           ├─ model_pdf_content.py
│  │           └─ model_pdf_metadata.py
│  ├─ db
│  │  └─ airflow.db
│  ├─ docker-compose.yaml
│  └─ requirements.txt
├─ architecture-diagram
│  ├─ flow_diagram.ipynb
│  ├─ flow_diagram.png
│  └─ input_icons
│     ├─ streamlit.png
│     └─ user.png
├─ backend
│  ├─ .dockerignore
│  ├─ Dockerfile
│  ├─ main.py
│  └─ requirements.txt
├─ docker-compose.yaml
├─ docker-compose.yml
├─ requirements.txt
├─ screenshots
│  └─ airflow-dag-run.png
|  └─ snowflake-loaded-data.png
|  └─ streamlit-file-upload.png
|  └─ streamlit-query-date.png
|  └─ sttreamlit-filter.png
└─ user-interface
   ├─ Dockerfile
   ├─ README.md
   ├─ airflow_trigger.py
   ├─ app.py
   ├─ dockerfile
   ├─ login_page.py
   ├─ main_page.py
   ├─ query_data.py
   ├─ register_page.py
   ├─ requirements.txt
   └─ upload_page.py
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)

**How to Run the Application Locally**
To run the application locally, follow these steps:

Clone the repository to get all the source code on your machine.

Use source/venv/bin/activate to activate the environment.

Create a .env file in the root directory with the following variables:

[AWS]
access_key = 
secret_key = 
region_name = 

[s3-bucket]
bucket = 

[SNOWFLAKE]
user = 
password = 
account =
warehouse =
database =
schema =
role =
content_table_name =
content_stage_name =
metadata_table_name =
metadata_stage_name =
pdf_data_file_format =

[MONGODB]
MONGODB_URL = 
DATABASE_NAME = 
COLLECTION_USER = 
COLLECTION_USER_FILE = 
 
[AIRFLOW]
AIRFLOW_URL =
AIRFLOW_DAG_ID = 
AIRFLOW_USERNAME = 
AIRFLOW_PASSWORD = 

Once you have set up your environment variables, Use docker-compose up - build to run the application

Access the Airflow UI by navigating to http://localhost:8080/ in your web browser.

Once the DAGs have run successfully, view the Streamlit application

Access the Streamlit UI by navigating to http://localhost:8503/ in your web browser.

## Learning Outcomes
**By completing this assignment, you will:**

**Cloud Services Deployment:**
- Deploy and manage applications on GCP Engines.
- Understand the benefits of using cloud services for scalability and reliability.

**Containerization with Docker:**
- Create, manage, and deploy Docker containers to encapsulate application environments.
- Utilize Docker for ensuring consistent deployments and isolating dependencies.

**Interactive Web Interface Creation:**
- Design and implement interactive web interfaces using frameworks like Streamlit.
- Handle file uploads and user input in a web application context.

**API Development:**
- Build RESTful APIs with FastAPI to handle web requests and automate backend processes.
- Integrate API endpoints with the user interface and processing pipeline.

**Automated Workflow Management:**
- Use Apache Airflow to automate and manage the workflow pipeline.
- Understand how to trigger and schedule tasks based on events or conditions.

**Data Extraction Techniques:**
- Develop scripts to extract data from PDF documents.
- Automate the process of extracting structured data from various document formats.

**Data Warehousing and ETL Processes:**
- Load and transform data into a data warehouse like Snowflake.
- Appreciate the role of ETL (Extract, Transform, Load) processes in data analytics.

**Data Security and Storage:**
- Manage secure storage of files using appropriate file storage solutions.
- Understand the considerations for data security in cloud-based storage.

These outcomes will equip learners with the skills and knowledge necessary to architect and implement scalable and efficient data processing systems in a cloud environment, with a focus on containerized applications and automated workflows.

## Team

| Name         | NUID          |
| ------------ | ------------- |
| Dongyu Liu   |  002837324    |
| Ekta Bhatia  |  002767736    |
| Parth Kalani |  002766306    |
| Sumit Sharma |  002778911    |
