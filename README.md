# Assignment04

## Related Link

[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=1rR8MdTSyWoAmdOa4enqDqAAH6V3XI_wwgrctNUDVrQQ/edit#4):link for codelab

[![Streamlit](https://camo.githubusercontent.com/2d35d09dad4cee1f9f94d8813d50c187602f2d319d36553cc576f827393182a0/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f53747265616d6c69742d4646344234423f7374796c653d666f722d7468652d6261646765266c6f676f3d53747265616d6c6974266c6f676f436f6c6f723d7768697465)]():link for streamlit service

[![Apache Airflow](https://camo.githubusercontent.com/e76401b08ad1964b782fb72c6d912ce1f68fc9d72e9b16a04859a61a0cb8d26a/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f416972666c6f772d3031374345453f7374796c653d666f722d7468652d6261646765266c6f676f3d417061636865253230416972666c6f77266c6f676f436f6c6f723d7768697465)]()

## Technologies Used

[![Amazon AWS](https://camo.githubusercontent.com/07b9bdb194e5e5789ccfc711011f139a64122ecf6f9f582c58d1b0658a267f98/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f416d617a6f6e5f4157532d4646393930303f7374796c653d666f722d7468652d6261646765266c6f676f3d616d617a6f6e617773266c6f676f436f6c6f723d7768697465)](https://aws.amazon.com/) [![Python](https://camo.githubusercontent.com/bb64b34d04a01cfa79658e2704085740d88e209c21905d0f5b55ebc87a83aa3a/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f507974686f6e2d4646443433423f7374796c653d666f722d7468652d6261646765266c6f676f3d707974686f6e266c6f676f436f6c6f723d626c7565)](https://www.python.org/) [![Apache Airflow](https://camo.githubusercontent.com/e76401b08ad1964b782fb72c6d912ce1f68fc9d72e9b16a04859a61a0cb8d26a/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f416972666c6f772d3031374345453f7374796c653d666f722d7468652d6261646765266c6f676f3d417061636865253230416972666c6f77266c6f676f436f6c6f723d7768697465)](https://airflow.apache.org/) [![Docker](https://camo.githubusercontent.com/143024f77b90a42cca6ecc238a7d00d7d72c216cdacd319939d2d9d26748120a/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f446f636b65722d2532333234393645443f7374796c653d666f722d7468652d6261646765266c6f676f3d446f636b657226636f6c6f723d626c7565266c6f676f436f6c6f723d7768697465)](https://www.docker.com/) [![Google Cloud](https://camo.githubusercontent.com/90e06e46d213cea885f8f8ad83ba8dd50ea199b6ab308f9cc6fad723612aa53c/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f476f6f676c655f436c6f75642d2532333432383546342e7376673f7374796c653d666f722d7468652d6261646765266c6f676f3d676f6f676c652d636c6f7564266c6f676f436f6c6f723d7768697465)](https://cloud.google.com/) ![Snowflake](https://camo.githubusercontent.com/0dbe98edfb3f64ca11a164e3e6bb92b5c33fe90726c1dd16ec29d6d218c77909/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f736e6f77666c616b652d2532333432383546343f7374796c653d666f722d7468652d6261646765266c6f676f3d736e6f77666c616b65266c696e6b3d68747470732533412532462532467777772e736e6f77666c616b652e636f6d253246656e2532462533465f6761253344322e34313530343830352e3636393239333936392e313730363135313037352d313134363638363130382e313730313834313130332532365f676163253344312e3136303830383532372e313730363135313130342e436a304b4351694168384f74426843514152497341496b576236386a354e7854366c716d4856626147647a51594e537a37553063665243732d53546a785a746750635a45562d325673322d6a38484d614171507345414c775f776342266c6f676f436f6c6f723d7768697465)

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



## Setup and Execution

### **Before running :**

Python: Ensure Python is installed on your system.

Docker: Ensure Docker-desktop is installed on your system.

Google Cloud Platform: Create a Google Cloud Engine. Ensure you have the necessary credentials and configurations set up in the configurations.properties file.

### Run the code locally:

Clone the repository to get all the source code on your machine.

Use `source /venv/bin/activate` to activate the environment.

Create a `.env` file in the root directory with the following variables:

```
[airflow]
AIRFLOW_UID=
AIRFLOW_PROJ_DIR=
[snowflake]
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
[AWS s3]
AWS_SK=
AWS_AK=
BUCKET_NAME=
```

Once you have set up your environment variables, Use `make build-up` to create the docker image and run it.

Access the Streamlit UI by navigating to [0.0.0.0:8503]() in you browser.

Access the Airflow UI by navigating to [0.0.0.0:8080]() in your browser.



## Program Structure

```
📦 Assignment04
├─ .gitignore
├─ Makefile
├─ README.md
├─ docker-compose.yaml
├─ streamlit
│  ├─ .gitignore
│  ├─ __init__.py
│  ├─ config
│  ├─ dockerfile
│  ├─ main.py
│  └─ requirements.txt
├─ fastapi
│  ├─ .gitignore
│  ├─ dockerfile
│  ├─ main.py
│  └─ requirements.txt
├─ airflow
│  ├─ airflow.cfg
│  ├─ dockerfile
│  ├─ docker-compose.yaml
│  ├─ logs
│  │  └─ scheduler
│  │     └─ latest
│  └─ dags
│     └─ pdf_processing_dag.py
├─ FastAPI2
│  ├─ .gitgnore
│  ├─ dockerfile
│  ├─ main.py
│  └─ requirements.txt
└─ diagrams
   ├─ airflow.png
   ├─ architecture-diagram.ipynb
   ├─ data_architecture_diagram.png
   ├─ docker.png
   ├─ fastapi.png
   ├─ pydantic-logo.png
   ├─ snowflake.png
   ├─ sqlalchemy.png
   └─ streamlit.png
```


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

| Name         | NUID      |
| ------------ | --------- |
| Dongyu Liu   | 002837324 |
| Ekta Bhatia  | 002767736 |
| Parth Kalani | 002766306 |
| Sumit Sharma | 002778911 |