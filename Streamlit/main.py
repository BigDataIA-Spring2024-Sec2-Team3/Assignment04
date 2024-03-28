import streamlit as st
import requests
import boto3

# Streamlit UI
st.title("Welcome to our application")
st.header("Big Data Project - Team03")

# Number of PDF files input
number_of_files = st.number_input("Please enter the number of PDF files", min_value=1, max_value=10, step=1)

uploaded_files = None

if number_of_files > 0:
    uploaded_files = st.file_uploader("Upload PDF files", type="pdf", accept_multiple_files=True)

    if uploaded_files is not None and len(uploaded_files) == number_of_files:
        st.success("Uploading files...")
        
        # Hardcoded S3 bucket name
        BUCKET_NAME = "assignment04team03"
        
        # Create an S3 resource
        s3_resource = boto3.resource('s3')

        # Iterate through the uploaded files and upload to S3
        for file in uploaded_files:
            file_bytes = file.read()
            file_name = file.name  # Get the original file name

            # Upload the file to S3
            s3_resource.Bucket(BUCKET_NAME).put_object(
                Key=file_name,
                Body=file_bytes
            )

        st.success("All files uploaded!")

        # Trigger FastAPI service and provide S3 location
        s3_location = f"s3://{BUCKET_NAME}/"  # Construct S3 location
        #response = requests.post("http://localhost:8000/trigger-airflow/", json={"s3_location": s3_location})
        response = requests.post("http://fastapi:8000/trigger-airflow/", json={"s3_location": s3_location})
        if response.status_code == 200:
            st.success("FastAPI triggered successfully!")
        else:
            st.error("Failed to trigger FastAPI. Please try again.")
    elif uploaded_files is not None:
        st.warning(f"Please upload exactly {number_of_files} files.")
