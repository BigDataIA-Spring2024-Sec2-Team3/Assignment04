import streamlit as st
import requests
import boto3
from dotenv import load_dotenv
import os

load_dotenv(override=True)

# Streamlit UI
st.title("Welcome to our application")
st.header("Big Data Project - Team03")
st.subheader('Snowflake SQL Executor')



# FastAPI's URL
FASTAPI_URL = 'http://localhost:8501/query/'

# input SQL 
# sql_query = st.text_area("SQL Query", height=300)
sql_query = """
select * from PDF_DATA.PUBLIC.PDF_METADATA;
"""
# st.write(sql_query)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD =  os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# st.write(SNOWFLAKE_PASSWORD)
# st.write(SNOWFLAKE_ACCOUNT)
# st.write(SNOWFLAKE_USER)
# st.write(SNOWFLAKE_WAREHOUSE)
# st.write(SNOWFLAKE_SCHEMA)
# st.write(sql_query)

# execute button
if st.button('Run Query'):
    with st.spinner("Loading"):
        response = requests.post(FASTAPI_URL, json={"sql": sql_query})
        st.write(response.json())
        try:
            if response.status_code == 200:
                data = response.json()["data"]
                st.write(data)
        except Exception as e:
                st.error("Failed to fetch data: {e}")