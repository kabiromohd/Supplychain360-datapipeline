import gspread
import pandas as pd
import os
import logging
from google.oauth2.service_account import Credentials
from launchpad_capstone.include.logger import get_logger

logger = get_logger(__name__)

OUTPUT_DIR = "/opt/airflow/dags/launchpad_capstone/raw_stores_details"
OUTPUT_FILE = "stores_details"

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_stores_details():
    """Extract stores details from Google Sheets and save as CSV file in local directory.
    Checks if the file already exists to avoid re-downloading unchanged data.
    """

    filepath = f"{OUTPUT_DIR}/{OUTPUT_FILE}.csv"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(filepath):
        logger.info("Downloading Raw Stores Details")
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        
        credentials = Credentials.from_service_account_file(
            "/opt/airflow/dags/launchpad_capstone/credentials.json",
            scopes=SCOPES
        )
        
        client = gspread.authorize(credentials)
        
        sheet = client.open_by_key("1-TKc698kfM7f2Yo39CWFDaRhmkYzvgfiTVlo1vxvd7Q")
        
        worksheet = sheet.sheet1
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        df.to_csv(filepath, index = False)
        logger.info("Stores Details file Ingested")
    else:
        logger.info("Stores Details file already exists.. ingestion terminated ")

def extract_stores_details_wrapper():
    """
    Wrapper function for extract_stores_details to handle exceptions and log errors.
    """
    try:
        extract_stores_details()
    except Exception as e:
        logger.error(f"Error in extract_stores_details_wrapper: {e}")
        raise