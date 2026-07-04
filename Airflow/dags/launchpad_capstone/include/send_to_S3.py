from datetime import datetime
import os
import json
import io
import pandas as pd
import boto3
import logging
from airflow.sdk import Variable
from launchpad_capstone.include.logger import get_logger

logger = get_logger(__name__)

# Load AWS credentials from Airflow Variables
Access_key = Variable.get("access_key_2", deserialize_json=True)
AWS_ACCESS_KEY = Access_key["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = Access_key["AWS_SECRET_KEY"]
REGION = Access_key["REGION"]

BUCKET_NAME = "launchpad-redshift-spectrum-kabir"

def enforce_shipments_schema(df, file_name):
    """Enforces the schema for the shipments_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["shipment_id", "warehouse_id", "store_id", "product_id", "quantity_shipped",
     "shipment_date", "expected_delivery_date", "actual_delivery_date", "carrier"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated()
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate rows found shipment data")
        logger.info(duplicates)
        df = df.drop_duplicates()
        logger.info("Duplicates removed. Proceeding with schema enforcement.")     
    
    # Enforce schema
    logger.info(f"Enforcing schema for shipments data {file_name}")
    df = df.astype({
        "shipment_id": "object",
        "warehouse_id": "object",
        "store_id": "object",
        "product_id": "object",
        "quantity_shipped": "int64",
        "carrier": "object"
    })
    date_cols = ["shipment_date", "expected_delivery_date", "actual_delivery_date"]

    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col],
            format="%Y-%m-%dT%H:%M:%S",
            errors="raise"
        )
    return df


def enforce_warehouse_schema(df, file_name):
    """Enforces the schema for the warehouses_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["warehouse_id", "city", "state"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated(subset=["warehouse_id"], keep=False)
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate rows found in warehouses data")
        logger.info(duplicates) 
        df = df.drop_duplicates(subset=["warehouse_id"])
        logger.info("Duplicates removed. Proceeding with schema enforcement.")
    
    # Enforce schema
    logging.info(f"Enforcing schema for warehouses data {file_name}")
    df = df.astype({
        "warehouse_id": "object",
        "city": "object",
        "state": "object"
    })
    return df

def enforce_suppliers_schema(df, file_name):
    """Enforces the schema for the suppliers_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["supplier_id", "supplier_name", "category", "country"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated(subset=["supplier_id"], keep=False)
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate rows found in suppliers data")
        logger.info(duplicates) 
        df = df.drop_duplicates(subset=["supplier_id"])
        logger.info("Duplicates removed. Proceeding with schema enforcement.")
    
    # Enforce schema
    logging.info("Enforcing schema for suppliers data")
    df = df.astype({
        "supplier_id": "object",
        "supplier_name": "object",
        "category": "object",
        "country": "object"
    })
    return df

def enforce_products_schema(df, file_name):
    """Enforces the schema for the products_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["product_id", "product_name", "category", "brand", "supplier_id", "unit_price"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated(subset=["product_id"], keep=False)
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate rows found in products data")
        logger.info(duplicates) 
        df = df.drop_duplicates(subset=["product_id"])
        logger.info("Duplicates removed. Proceeding with schema enforcement.")
    
    # Enforce schema
    logging.info(f"Enforcing schema for products data {file_name}")
    df = df.astype({
        "product_id": "object",
        "product_name": "object",
        "category": "object",
        "brand": "object",
        "supplier_id": "object",
        "unit_price": "float64"
    })
    return df

def enforce_inventory_schema(df, file_name):
    """Enforces the schema for the inventory_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["warehouse_id", "product_id", "quantity_available", "reorder_threshold", "snapshot_date"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated()
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate rows found in inventory data")
        logger.info(duplicates) 
        df = df.drop_duplicates()
        logger.info("Duplicates removed. Proceeding with schema enforcement.")  
    
    # Enforce schema
    logging.info(f"Enforcing schema for inventory data {file_name}")
    df = df.astype({
        "warehouse_id": "object",
        "product_id": "object",
        "quantity_available": "int64",
        "reorder_threshold": "int64"
    })
    df["snapshot_date"] = pd.to_datetime(
                    df["snapshot_date"],
                    format="%Y-%m-%d",
                    errors="raise"   # fail fast if bad data
                )
    return df


def enforce_stores_sales_schema(df, file_name):
    """Enforces the schema for the stores_sales_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["transaction_id", "store_id", "product_id",
     "quantity_sold", "unit_price", "discount_pct", "sale_amount", "transaction_timestamp"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated(subset=["transaction_id"], keep=False)
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate transaction_id values found in stores_sales data")
        logger.info(duplicates) 
        df = df.drop_duplicates(subset=["transaction_id"])
        logger.info("Duplicates removed. Proceeding with schema enforcement.")
    
    # Enforce schema
    logging.info(f"Enforcing schema for stores_sales data {file_name}")
    df = df.astype({
        "transaction_id": "object",
        "store_id": "object",
        "product_id": "object",
        "quantity_sold": "int64",
        "unit_price": "float64",
        "discount_pct": "float64",
        "sale_amount": "float64"
    })

    # Convert date (DD/MM/YYYY → datetime)
    df["transaction_timestamp"] = pd.to_datetime(
        df["transaction_timestamp"],
        format="%Y-%m-%d %H:%M:%S",
        errors="raise"   # fail fast if bad data
    )
    return df


def enforce_stores_details_schema(df, file_name):
    """Enforces the schema for the stores_details_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["store_id", "store_name", "city", "state", "region", "store_open_date"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated(subset=["store_id"], keep=False)
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicate store_id values found in stores_details data")
        logger.info(duplicates) 
        df = df.drop_duplicates(subset=["store_id"])
        logger.info("Duplicates removed. Proceeding with schema enforcement.")
    
    # Enforce schema
    logging.info(f"Enforcing schema for stores_details data {file_name}")
    df = df.astype({
        "store_id": "object",
        "store_name": "object",
        "city": "object",
        "state": "object",
        "region": "object"
    })

    # Convert date (DD/MM/YYYY → datetime)
    df["store_open_date"] = pd.to_datetime(
        df["store_open_date"],
        format="%d/%m/%Y",
        errors="raise"   # fail fast if bad data
    )
    return df


def set_schema_wrapper(df, file_name):
    """
    SGeneric schema enforcement function that applies transformations based on the target table.
    """
    if file_name.startswith("stores_details"):
        return enforce_stores_details_schema(df, file_name)

    if file_name.startswith("sales_"):
        return enforce_stores_sales_schema(df, file_name)

    if file_name.startswith("inventory_"):
        return enforce_inventory_schema(df, file_name)  

    if file_name.startswith("products"):
        return enforce_products_schema(df, file_name)

    if file_name.startswith("suppliers"):
        return enforce_suppliers_schema(df, file_name)

    if file_name.startswith("warehouses"):
        return enforce_warehouse_schema(df, file_name)

    if file_name.startswith("shipments"):
        return enforce_shipments_schema(df, file_name)  

    else:
        logger.warning(f"No specific schema enforcement defined for {file_name}. Applying generic enforcement.")
        raise ValueError(f"Unrecognized file prefix for schema enforcement: {file_name}")
    return df  


def upload_to_s3(df, parquet_file_name, file_path, S3_PREFIX, s3_client):
    """
    Uploads a DataFrame as a Parquet file to S3.
   """
    
    # In-memory conversion (no /tmp)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_key = f"{S3_PREFIX}/{parquet_file_name}"

    # Upload to S3
    s3_client.upload_fileobj(buffer, BUCKET_NAME, s3_key)

    logging.info(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")

    # Remove the original CSV file after processing
    logging.info(f"Removing original file: {file_path}")
    os.remove(file_path)


def csv_to_parquet_s3(path_processed, folder_processed):
    """
    Main function to process CSV/JSON files and upload as Parquet to S3.
    """
    
    INPUT_DIR = path_processed

    S3_PREFIX = f"{folder_processed}/"
    prefixes = ("shipments_", "sales_")


    # Initialize S3 client with credentials
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION
    )
    
    if not os.path.exists(INPUT_DIR):
        logging.error(f"Input directory does not exist: {INPUT_DIR}")
        return
    
    if not os.listdir(INPUT_DIR):
        logging.warning(f"No files found in input directory: {INPUT_DIR} to process.")
        return
    
    for file_name in os.listdir(INPUT_DIR):
        if file_name.endswith(".csv"):
            file_path = os.path.join(INPUT_DIR, file_name)

            logging.info(f"Processing {file_name}")

            df = pd.read_csv(file_path)
            
            df = set_schema_wrapper(df, file_name)

            if file_name.startswith(prefixes):
                # get date part of filename
                date_part = file_name.split('_')[1].replace('.csv', '')
        
                # insert date column at index 0
                df["file_date"] = date_part

                # Convert date (DD/MM/YYYY → datetime)
                df["file_date"] = pd.to_datetime(
                    df["file_date"],
                    format="%Y-%m-%d",
                    errors="raise"   # fail fast if bad data
                )
                
            parquet_file = file_name.replace(".csv", ".parquet")
            upload_to_s3(df, parquet_file, file_path, S3_PREFIX, s3_client)
        
        elif file_name.endswith(".json"):
            file_path = os.path.join(INPUT_DIR, file_name)

            logging.info(f"Processing {file_name}")

            with open(file_path) as f:
                data = json.load(f)

            df = pd.json_normalize(data)

            df = set_schema_wrapper(df, file_name)

            if file_name.startswith(prefixes):
                # get date part of filename
                date_part = file_name.split('_')[1].replace('.json', '')

                # insert date column at index 0
                df["file_date"] = date_part

                # Convert date (DD/MM/YYYY → datetime)
                df["file_date"] = pd.to_datetime(
                    df["file_date"],
                    format="%Y-%m-%d",
                    errors="raise"   # fail fast if bad data
                )

            parquet_file = file_name.replace(".json", ".parquet")
            upload_to_s3(df, parquet_file, file_path, S3_PREFIX, s3_client)

        else:
            logging.warning(f"Unsupported file format: {file_name}. Skipping.") 



def send_to_S3_wrapper(**kwargs):
    """
    Wrapper function to call the main processing function.
    """
    
    logging.info("Starting send_to_S3_wrapper")
    path_processeds = ["/opt/airflow/dags/launchpad_capstone/raw_files/inventory/",
                        "/opt/airflow/dags/launchpad_capstone/raw_files/products/",
                        "/opt/airflow/dags/launchpad_capstone/raw_files/shipments/",
                        "/opt/airflow/dags/launchpad_capstone/raw_files/suppliers/",
                        "/opt/airflow/dags/launchpad_capstone/raw_files/warehouses/",
                         "/opt/airflow/dags/launchpad_capstone/raw_stores_details/",
                         "/opt/airflow/dags/launchpad_capstone/raw_store_sales/"
                         ]

    folder_processeds = ["inventory",
                        "products",
                         "shipments",
                          "suppliers",
                           "warehouses",
                            "stores_details",
                             "stores_sales"
                             ]

    for path, folder in zip(path_processeds, folder_processeds):
        try:
            csv_to_parquet_s3(path, folder)
        except Exception as e:
            logging.error(f"Error in send_to_S3_wrapper: {e}")
            raise
       

