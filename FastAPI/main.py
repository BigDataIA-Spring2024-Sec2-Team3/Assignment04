from fastapi import FastAPI, HTTPException
import requests  # To make HTTP requests to Airflow's REST API
import os
from pydantic import BaseModel
from dotenv import load_dotenv
load_dotenv(override=True)

class Files(BaseModel):
    file_keys:list[str]
    user_id:int

app = FastAPI()


@app.post("/trigger-airflow/")
async def trigger_pdf_processing(files: Files):
    bucket_name = os.getenv("BUCKET_NAME")
    file_keys = files.file_keys
    user_id = files.user_id
    # Construct the payload for the Airflow DAG trigger
    # for file_key in file_keys:
    dag_trigger_payload = {
        "conf": {
            "bucket_name": bucket_name,
            "file_keys": file_keys,
            "user_id": user_id
        }
    }

    # URL for the Airflow REST API endpoint to trigger a DAG
    airflow_dag_trigger_url = "http://assignment04-airflow-webserver-1:8080/api/v1/dags/handle_pdf_dag/dagRuns"
    
    # Authentication for Airflow (adjust as needed)
    airflow_auth = ("airflow", "airflow")
    try:
        # Make the request to Airflow to trigger the DAG
        response = requests.post(
            airflow_dag_trigger_url,
            json=dag_trigger_payload,
            auth=airflow_auth
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        # HTTP error
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        # other error
        print(f"An error occurred: {err} with file list {file_keys}")
    return {
        "message": f"DAG triggered successfully, upload {file_keys}",
        }