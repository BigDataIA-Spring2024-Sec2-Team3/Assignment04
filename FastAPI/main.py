
# from fastapi import FastAPI, HTTPException
# import requests

# app = FastAPI()

# @app.post("/trigger-airflow/")
# async def trigger_airflow(s3_locations: list[str]) -> dict:
#     print("Received S3 locations:", s3_locations)
#     airflow_api_url = "http://airflow:8080/api/v1/dags/pdf_processing_dag/dagRuns"

#     for s3_location in s3_locations:
#         payload = {
#             "conf": {"s3_location": s3_location}
#         }

#         try:
#             response = requests.post(airflow_api_url, json=payload)

#             if response.status_code == 200:
#                 return {"message": "DAG run triggered successfully"}
#             else:
#                 raise HTTPException(status_code=response.status_code, detail=response.text)
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

from fastapi import FastAPI, HTTPException
import requests
from requests.auth import HTTPBasicAuth

app = FastAPI()

AIRFLOW_BASE_URL = "http://localhost:8080/api/v1/dags"
USERNAME = "airflow"
PASSWORD = "airflow"

@app.post("/trigger-dag/")
async def trigger_dag(dag_id: str, s3_locations: list[str]) -> dict:
    print("Received S3 locations:", s3_locations)

    try:
        dag_trigger_url = f"{AIRFLOW_BASE_URL}/{dag_id}/dag_runs"
        auth = HTTPBasicAuth(USERNAME, PASSWORD)

        for s3_location in s3_locations:
            payload = {"conf": {"s3_location": s3_location}}
            response = requests.post(dag_trigger_url, json=payload, auth=auth)
            response.raise_for_status()

        return {"message": f"DAG {dag_id} triggered successfully"}
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")