"""
With this snippet you can connect to snowflake with python
More docs
https://docs.snowflake.com/en/user-guide/python-connector-pandas.html
In your .bashrc or .zshrc make sure you add the following variables
export user='MYUSERNAME'
export password='MYPASSWORD'
export account='hi68877'
Dependecies to run the script:
pip install "snowflake_connector_python[pandas]==2.8.2"
pip install jwt==1.3.1
To run it (after doing the other tasks):
python worksheets/connect.py
account url: https://dw27462.sa-east-1.aws.snowflakecomputing.com
NSWUBBF.MW32403
"""
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

path_to_env = "D:/gnavarro/Escritorio/Courses/Learn Data Engineering/Fundamental Tools/Snowflake for Data Engineers/snowflake-for-data-engineers-main/.env"

load_dotenv(dotenv_path=path_to_env)

print("starting connection")
region = "sa-east-1"
snowflake_id = os.getenv("account")
account_url = f"{snowflake_id}.snowflakecomputing.com"
print(f"Connecting to {account_url}")

ctx = snowflake.connector.connect(
    user=os.getenv("user"),
    password=os.getenv("password"),
    region=region,
    account=snowflake_id,
    timeout=10,
    warehouse="SMALLWAREHOUSE",
    database="TESTDB",
    schema="ECOMMERCE",
    autocommit=True,
)

print("ok")

db_cursor_eb = ctx.cursor()
res = db_cursor_eb.execute(
    """
SELECT CUSTOMERID, COUNT(DISTINCT INVOICENO) AS N_ORDERS
FROM INVOICES
GROUP BY COUNTRY, CUSTOMERID
ORDER BY N_ORDERS DESC
LIMIT 10;
"""
)
# Fetches all records retrieved by the query and formats them in pandas DataFrame
df = res.fetch_pandas_all()
print(df)
