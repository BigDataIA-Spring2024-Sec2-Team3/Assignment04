from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests

app = FastAPI()

# Define a Pydantic model for the S3 location
class S3Location(BaseModel):
    s3_location: str

# Define FastAPI endpoint for triggering Airflow with S3 location
# @app.post("/trigger-airflow/")
# async def trigger_airflow(s3_location: S3Location) -> dict:
#     # Assuming Airflow's REST API endpoint for triggering DAGs is /api/v1/dags/<dag_id>/dagRuns
#     airflow_api_url = "http://airflow:8081/api/v1/dags/sandbox/dagRuns"

#     # Construct the request payload
#     payload = {
#         "conf": {"s3_location": s3_location.s3_location},
#         "execution_date": "2024-03-27T00:00:00Z"  # Specify the execution date if needed
#     }

#     # Send a POST request to trigger the DAG run
#     response = requests.post(airflow_api_url, json=payload)

#     # Check if the request was successful
#     if response.status_code == 200:
#         return {"message": f"Airflow triggered with S3 location: {s3_location.s3_location}"}
#     else:
#         raise HTTPException(status_code=response.status_code, detail=response.text)
@app.post("/trigger-airflow/")
async def trigger_airflow(s3_location: S3Location) -> dict:
    # Construct the Airflow API URL
    airflow_api_url = "http://localhost:8081/api/v1/dags/sandbox/dagRuns"
    
    # Construct the request payload
    payload = {
        "conf": {"s3_location": s3_location.s3_location}
        #,
        #"execution_date": "2024-03-27T00:00:00Z"
    }

    try:
        # Send a POST request to trigger the DAG run
        response = requests.post(airflow_api_url, json=payload)

        # Check if the request was successful
        if response.status_code == 200:
            return {"message": "DAG run triggered successfully"}
        else:
            return {"error": "Failed to trigger DAG run"}
    except Exception as e:
        return {"error": f"An error occurred: {e}"}
