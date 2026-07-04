import boto3
import psycopg2
import json
import os
import logging
import re
from airflow.sdk import Variable
from launchpad_capstone.include.logger import get_logger

logger = get_logger(__name__)

# Load AWS credentials from Airflow Variables
Access_key = Variable.get("access_key", deserialize_json=True)
AWS_ACCESS_KEY = Access_key["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = Access_key["AWS_SECRET_KEY"]
REGION = Access_key["REGION"]

STATE_FILE = "/opt/airflow/dags/launchpad_capstone/Sales_trxns_details_state_file.json"
OUTPUT_DIR = "/opt/airflow/dags/launchpad_capstone/raw_store_sales"

def extract_transactions_details():
    """Extract transactions details from Postgres and save as CSV files in local directory.
    Uses a state file to track ingested tables and avoid re-processing already ingested data."""

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION
    )

    ssm = session.client("ssm")

    params = [
        "/supplychain360/db/host",
        "/supplychain360/db/port",
        "/supplychain360/db/dbname",
        "/supplychain360/db/user",
        "/supplychain360/db/password"
    ]

    response = ssm.get_parameters(
        Names=params,
        WithDecryption=True
    )

    p = {x["Name"]: x["Value"] for x in response["Parameters"]}

    db_host = p["/supplychain360/db/host"]
    db_port = p["/supplychain360/db/port"]
    db_name = p["/supplychain360/db/dbname"]
    db_user = p["/supplychain360/db/user"]
    db_password = p["/supplychain360/db/password"]

    with psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    ) as conn:    
        conn.autocommit = True
        
        with conn.cursor() as cursor:

            query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name ILIKE 'sales_%'
            """

            cursor.execute(query)

            latest_tables = cursor.fetchall()

            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r") as f:
                    ingested = json.load(f)
            else:
                ingested = []

            for latest_table in latest_tables:
                
                latest_table = list(latest_table)[0]
                if latest_table in ingested:
                    print("Table {latest_table} already ingested. Exists.")
                else:
                    # Replace only the underscores between numbers
                    new_file_name = re.sub(r'(\d{4})_(\d{2})_(\d{2})', r'\1-\2-\3', latest_table)
                    
                    filepath = f"{OUTPUT_DIR}/{new_file_name}.csv"

                    with open(filepath, "w") as f:
                        cursor.copy_expert(
                            f"COPY public.{latest_table} TO STDOUT WITH CSV HEADER",
                            f
                        )
                
                    logger.info(f"Exported: {latest_table}")
                
                    ingested.append(latest_table)

            logger.info("Saving database STATE_FILE")
            with open(STATE_FILE, "w") as f:
                json.dump(ingested, f, indent=2)

def extract_transactions_details_wrapper(**kwargs):
    """
    Wrapper function to extract transactions details. 
    """
    extract_transactions_details()