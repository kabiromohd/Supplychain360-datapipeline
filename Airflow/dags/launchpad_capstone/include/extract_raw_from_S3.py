import boto3
import os
import json
import logging
from airflow.sdk import Variable
from launchpad_capstone.include.logger import get_logger

logger = get_logger(__name__)

# Load AWS credentials from Airflow Variables
Access_key = Variable.get("access_key", deserialize_json=True)
AWS_ACCESS_KEY = Access_key["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = Access_key["AWS_SECRET_KEY"]
REGION = Access_key["REGION"]

BUCKET = "supplychain360-data"
PREFIX = "raw/"

LOCAL_DIR = "/opt/airflow/dags/launchpad_capstone/raw_files/"
STATE_FILE = "/opt/airflow/dags/launchpad_capstone/raw_S3_processed_state_file.json"

def extract_raw_file_s3():
    """Extract raw files from S3 and save to local directory, preserving folder structure.
    Uses a state file to track processed files and avoid re-downloading unchanged files."""
    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Load processed files state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            processed = json.load(f)
    else:
        processed = {}

    # AWS session
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION
    )

    s3 = session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if key.endswith("/"):
                continue

            if not (key.endswith(".csv") or key.endswith(".json")):
                continue

            etag = obj["ETag"]

            # Check if file already processed
            if key in processed and processed[key] == etag:
                logger.info(f"Skipping already downloaded file: {key}")
                continue

            # Preserve folder structure
            relative_path = os.path.relpath(key, PREFIX)
            local_path = os.path.join(LOCAL_DIR, relative_path)

            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            logger.info(f"Downloading {key} -> {local_path}")

            s3.download_file(BUCKET, key, local_path)

            # Update state
            processed[key] = etag

    # Save updated state
    with open(STATE_FILE, "w") as f:
        json.dump(processed, f, indent=2)

    logger.info("Download job finished")

def download_wrapper_s3(**kwargs):
    """
    Wrapper function to extract raw files from S3. 
    """
    extract_raw_file_s3()