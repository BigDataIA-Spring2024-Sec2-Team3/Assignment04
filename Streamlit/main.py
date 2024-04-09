# import streamlit as st
# import requests
# import boto3
# from dotenv import load_dotenv
# import os
# import snowflake.connector

# load_dotenv(override=True)

# ak = os.getenv("AWS_SK")
# aki = os.getenv("AWS_AK")


# # Streamlit UI
# st.title("Big Data Project - Team03")
# st.header("Welcome to our PDF Parser")
# # st.subheader("Airflow work flow")
# # Number of PDF files input
# number_of_files = st.number_input("Please enter the number of PDF files", min_value=1, max_value=10, step=1)

# uploaded_files = None
# # only run once for initialize
# @st.cache_resource
# def initialize():
#     # drop table function
#     SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
#     SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
#     SNOWFLAKE_PASSWORD =  os.getenv("SNOWFLAKE_PASSWORD")
#     SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
#     SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
#     SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
#     sql = [
#     'DROP TABLE if exists PDF_CONTENTS;',
#     'DROP TABLE if exists PDF_METADATA;',
#     'DROP TABLE if exists TEST_PDF_CONTENTS;',
#     'DROP TABLE if exists TEST_PDF_METADATA;',
#     ]
#     try:
#         conn = snowflake.connector.connect(
#             user=SNOWFLAKE_USER,
#             password=SNOWFLAKE_PASSWORD,
#             account=SNOWFLAKE_ACCOUNT,
#             warehouse=SNOWFLAKE_WAREHOUSE,
#             database=SNOWFLAKE_DATABASE,
#             schema=SNOWFLAKE_SCHEMA
#         )
#         cursor = conn.cursor()
#         for q in sql:
#             cursor.execute(q)
#         result = cursor.fetchall()
#         cursor.close()
#         conn.close()
#     except Exception as e:
#         st.error(f'Failed{e}')
#         exit(1)

# initialize()


# if number_of_files > 0:
#         uploaded_files = st.file_uploader("Upload PDF files", type="pdf", accept_multiple_files=True)

# if st.button("Upload"):
#     if uploaded_files is not None and len(uploaded_files) == number_of_files:
#         st.success("Uploading files...")
        
#         # Hardcoded S3 bucket name
#         BUCKET_NAME = os.getenv("BUCKET_NAME")
        
#         # Create an S3 resource
#         s3_resource = boto3.resource('s3',aws_access_key_id = aki, aws_secret_access_key =ak)

#         # Iterate through the uploaded files and upload to S3
#         s3_keys = []  # Store S3 URLs
#         for file in uploaded_files:
#             file_bytes = file.read()
#             file_name = file.name  # Get the original file name

#             # Upload the file to S3
#             s3_resource.Bucket(BUCKET_NAME).put_object(
#                 Key=file_name,
#                 Body=file_bytes
#             )
            
#             # Construct S3 URL
#             s3_key = f"{file_name}"
#             s3_keys.append(s3_key)

#         s3_urls = {
#             'file_keys':s3_keys
#         }

#         # st.write(s3_urls)
#         st.success("All files uploaded!")



#         # Trigger FastAPI service and provide S3 locations
#         try:
#             response = requests.post("http://fastapi:8000/trigger-airflow/", json=s3_urls)
#             response.raise_for_status()  # Raise exception for non-200 status codes
#             st.success(response.json().get('message'))
#         except requests.exceptions.HTTPError as err:
#             error_detail = err.response.json().get('detail', 'Unknown error')  # get the detail of error, if not print "Unknow error"
#             st.error(f"Error: {error_detail}")  # show streamlit
#         except Exception as e:
#             st.error(f"An error occurred: {e}")
#     elif uploaded_files is not None:
#         st.warning(f"Please upload exactly {number_of_files} files.")

import streamlit as st
import requests
import boto3
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv(override=True)

ak = os.getenv("AWS_SK")
aki = os.getenv("AWS_AK")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")

# Function to connect to Snowflake
def connect_to_snowflake():
    ctx = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema
    )
    return ctx

# Streamlit UI
st.title("Big Data Project - Team03")
st.header("Welcome to our application for PDF processing")

# Check if the user is logged in
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# User login
username = st.text_input("Username")
password = st.text_input("Password", type="password")

if st.button("Login"):
    # Validate user credentials by querying the database
    if not username or not password:
        st.error("Please enter both username and password.")
    else:
        try:
            conn = connect_to_snowflake()
            cursor = conn.cursor()
            cursor.execute(f"SELECT user_id, password FROM users WHERE username = '{username}'")
            result = cursor.fetchone()
            if result:
                user_id, storedpassword = result
                if password == storedpassword:
                    st.session_state.user_id = user_id
                    st.success("Login successful!")
                    st.session_state.logged_in = True
                else:
                    st.error("Incorrect password")
            else:
                st.error("Invalid username")
        except Exception as e:
            st.error(f"An error occurred: {e}")
        finally:
            cursor.close()
            conn.close()

# User sign-up
if not st.session_state.logged_in:
    new_username = st.text_input("New Username")
    new_password = st.text_input("New Password", type="password")

    if st.button("Sign Up"):
        if not new_username or not new_password:
            st.error("Please enter both username and password.")
        else:
            try:
                conn = connect_to_snowflake()
                cursor = conn.cursor()
                cursor.execute(f"INSERT INTO users (username, password) VALUES ('{new_username}', '{new_password}')")
                conn.commit()
                st.success("Sign-up successful! Please proceed to login.")
            except Exception as e:
                st.error(f"An error occurred: {e}")
            finally:
                cursor.close()
                conn.close()

if st.session_state.logged_in:
    # Number of PDF files input
    number_of_files = st.number_input("Please enter the number of PDF files", min_value=1, max_value=10, step=1)

    uploaded_files = None

    if number_of_files > 0:
        uploaded_files = st.file_uploader("Upload PDF files", type="pdf", accept_multiple_files=True)

    if st.button("Upload"):
        if uploaded_files is not None and len(uploaded_files) == number_of_files:
            st.success("Uploading files...")
            
            # Hardcoded S3 bucket name
            BUCKET_NAME = os.getenv("BUCKET_NAME")
            
            # Create an S3 resource
            s3_resource = boto3.resource('s3',aws_access_key_id = aki, aws_secret_access_key =ak)

            # Iterate through the uploaded files and upload to S3
            s3_keys = []  # Store S3 URLs
            for file in uploaded_files:
                file_bytes = file.read()
                file_name = file.name  # Get the original file name

                # Upload the file to S3
                s3_resource.Bucket(BUCKET_NAME).put_object(
                    Key=file_name,
                    Body=file_bytes
                )
                
                # Construct S3 URL
                s3_key = f"{file_name}"
                s3_keys.append(s3_key)

            s3_urls = {
                'file_keys':s3_keys,
                'user_id': st.session_state.user_id
            }

            #st.write(s3_urls)
            st.success("All files uploaded!")

            # Trigger FastAPI service and provide S3 locations
            try:
                response = requests.post("http://fastapi:8000/trigger-airflow/", json=s3_urls)
                response.raise_for_status()  # Raise exception for non-200 status codes
            except requests.exceptions.HTTPError as err:
                error_detail = err.response.json().get('detail', 'Unknown error')  # get the detail of error, if not print "Unknow error"
                st.error(f"Error: {error_detail}")  # show streamlit
            except Exception as e:
                st.error(f"An error occurred: {e}")

# Logout button
if st.session_state.logged_in:
    if st.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.user_id = None
        st.success("Logged out successfully!")