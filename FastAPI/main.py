'''from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests

app = FastAPI()

@app.post("/trigger-airflow/")
async def trigger_airflow(s3_locations: list[str]) -> dict:
    print("Received S3 locations:", s3_locations)
    airflow_api_url = "http://airflow:8081/api/v1/dags/sandbox/dagRuns"

    for s3_location in s3_locations:
        payload = {
            "conf": {"s3_location": s3_location}
        }

        try:
            response = requests.post(airflow_api_url, json=payload)

            if response.status_code == 200:
                return {"message": "DAG run triggered successfully"}
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {e}")'''

from fastapi import FastAPI, HTTPException
import requests

app = FastAPI()

@app.post("/trigger-airflow/")
async def trigger_airflow(s3_locations: list[str]) -> dict:
    print("Received S3 locations:", s3_locations)
    airflow_api_url = "http://airflow:8081/api/v1/dags/pdf_processing_dag/dagRuns"

    for s3_location in s3_locations:
        payload = {
            "conf": {"s3_location": s3_location}
        }

        try:
            response = requests.post(airflow_api_url, json=payload)

            if response.status_code == 200:
                return {"message": "DAG run triggered successfully"}
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {e}")