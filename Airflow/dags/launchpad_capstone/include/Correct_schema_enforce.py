import logging
import pandas as pd
from launchpad_capstone.include.logger import get_logger

logger = get_logger(__name__)

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
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate rows found in warehouses data")
    
    # Enforce schema
    df = df.astype({
        "shipment_id": "VARCHAR(200)",
        "warehouse_id": "VARCHAR(200)",
        "store_id": "VARCHAR(200)",
        "product_id": "VARCHAR(200)",
        "quantity_shipped": "INT",
        "shipment_date": "TIMESTAMP",
        "expected_delivery_date": "TIMESTAMP",
        "actual_delivery_date": "TIMESTAMP",
        "carrier": "VARCHAR(100)"
    })
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
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate rows found in warehouses data")
    
    # Enforce schema
    df = df.astype({
        "warehouse_id": "VARCHAR(200)",
        "city": "VARCHAR(100)",
        "state": "VARCHAR(100)"
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
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate rows found in suppliers data")
    
    # Enforce schema
    df = df.astype({
        "supplier_id": "VARCHAR(200)",
        "supplier_name": "VARCHAR(200)",
        "category": "VARCHAR(100)",
        "country": "VARCHAR(100)"
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
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate rows found in products data")
    
    # Enforce schema
    df = df.astype({
        "product_id": "VARCHAR(200)",
        "product_name": "VARCHAR(200)",
        "category": "VARCHAR(100)",
        "brand": "VARCHAR(100)",
        "supplier_id": "VARCHAR(200)",
        "unit_price": "DECIMAL(15,2)"
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
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate rows found in inventory data")
    
    # Enforce schema
    df = df.astype({
        "warehouse_id": "VARCHAR(200)",
        "product_id": "VARCHAR(200)",
        "quantity_available": "INT",
        "reorder_threshold": "INT",
        "snapshot_date": "DATE"
    })
    return df


def enforce_stores_sales_schema(df, file_name):
    """Enforces the schema for the stores_sales_table.
    - Ensures correct data types
    """
    if list(df.columns) != ["transaction_id", "store_id", "product_id", "quantity_sold", "unit_price", "discount_pct", "sale_amount", "transaction_timestamp"]:
        logger.error(f"schema drift detected!!!: {file_name}")
        raise ValueError(f"Unexpected columns in {file_name}: {df.columns.tolist()}")

    duplicate_mask = df.duplicated(subset=["transaction_id"], keep=False)
    duplicates = df[duplicate_mask]

    if not duplicates.empty:
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate transaction_id values found in stores_sales data")
    
    # Enforce schema
    df = df.astype({
        "transaction_id": "VARCHAR(200)",
        "store_id": "VARCHAR(200)",
        "product_id": "VARCHAR(200)",
        "quantity_sold": "INT",
        "unit_price": "DECIMAL(10,2)",
        "discount_pct": "DECIMAL(5,2)",
        "sale_amount": "DECIMAL(15,2)",
        "transaction_timestamp": "TIMESTAMP"
    })
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
        logger.info("Duplicates found:")
        logger.info(duplicates) 
        raise ValueError("Duplicate store_id values found in stores_details data")
    
    # Enforce schema
    df = df.astype({
        "store_id": "VARCHAR(200)",
        "store_name": "VARCHAR(200)",
        "city": "VARCHAR(100)",
        "state": "VARCHAR(100)",
        "region": "VARCHAR(100)"
    })

    # Convert date (DD/MM/YYYY → datetime)
    df["store_open_date"] = pd.to_datetime(
        df["store_open_date"],
        format="%d/%m/%Y",
        errors="raise"   # fail fast if bad data
    )

    # Format to YYYY-MM-DD string for Redshift compatibility
    df["store_open_date"] = df["store_open_date"].dt.strftime("%Y-%m-%d")
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