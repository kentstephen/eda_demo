import pandas as pd
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
project_ID = os.getenv('PROJECT_ID')
# Get the path to the JSON key file from the environment variable
key_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

sql_query = f'SELECT * FROM `{project_ID}.usa_names.names`'
df = pd.read_gbq(sql_query, project_id=project_ID, credentials=credentials)

import duckdb

con = duckdb.connect('C:\Project_Files\eda_demo\duckdb\demo.duckdb')
con.sql("create or replace table names as select * from df;")
con.close()