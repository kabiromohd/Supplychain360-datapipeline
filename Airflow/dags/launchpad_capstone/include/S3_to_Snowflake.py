import logging
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from launchpad_capstone.include.logger import get_logger

logger = get_logger(__name__)

BUCKET = "launchpad-redshift-spectrum-kabir"

def list_folders():
    """List unique folder prefixes in the specified S3 bucket."""

    s3 = S3Hook(aws_conn_id="aws_default")

    keys = s3.list_keys(bucket_name=BUCKET)

    folders = set()
    for key in keys:
        if "/" in key:
            folders.add(key.split("/")[0] + "/")

    return list(folders)


def create_tables(folders: list):
    """Create Snowflake tables based on the folder names in S3, 
    using the INFER_SCHEMA function to determine the structure of the data.
    """
    sf = SnowflakeHook(snowflake_conn_id="snowflake_default")

    for folder in folders:
        table = folder.replace("/", "")

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@s3_stage/{folder}',
                FILE_FORMAT => 'parquet_format'
            )
            )
        );
        """
        sf.run(create_sql)


def load_data(folders: list):
    """Load data from S3 into Snowflake tables using the COPY INTO command
    with the MATCH_BY_COLUMN_NAME option to ensure correct mapping of columns
    based on the inferred schema, and the FORCE option set to FALSE to avoid re-loading unchanged files.
    """

    sf = SnowflakeHook(snowflake_conn_id="snowflake_default")

    for folder in folders:
        table = folder.replace("/", "")

        copy_sql = f"""
        COPY INTO {table}
        FROM @s3_stage/{folder}
        FILE_FORMAT = (FORMAT_NAME = parquet_format)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FORCE = FALSE
        ON_ERROR = CONTINUE;
        """
        sf.run(copy_sql)


def validate_load(folders: list):
    """Validate that data was loaded correctly by comparing row counts in Snowflake tables
        with the number of files in the corresponding S3 folders.
    """

    sf = SnowflakeHook(snowflake_conn_id="snowflake_default")

    for folder in folders:
        table = folder.replace("/", "")
        result = sf.get_first(f"SELECT COUNT(*) FROM {table}")

        logger.info(f"{table} row count: {result[0]}")


def s3_to_snowflake_wrapper(**kwargs):
    """
    Wrapper function to execute the S3 to Snowflake data pipeline.
    """
    folders = list_folders()
    create_tables(folders)
    load_data(folders)
    validate_load(folders)
