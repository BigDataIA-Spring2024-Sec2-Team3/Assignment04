## snowflake query
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import snowflake.connector
from typing import List
from dotenv import load_dotenv
import os
load_dotenv(override=True)


app = FastAPI()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD =  os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

class Query(BaseModel):
    sql: str

@app.post("/query/")
async def run_query(query: Query):
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
        cursor.execute(query.sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
