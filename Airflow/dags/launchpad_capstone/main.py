import boto3
import json
import os
import requests
import time
import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
from datetime import timedelta
from airflow.sdk import Variable

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

from launchpad_capstone.include.extract_raw_from_S3 import download_wrapper_s3
from launchpad_capstone.include.extract_transactions_details import extract_transactions_details_wrapper
from launchpad_capstone.include.store_details_extract import extract_stores_details_wrapper
from launchpad_capstone.include.send_to_S3 import send_to_S3_wrapper
from launchpad_capstone.include.S3_to_Snowflake import s3_to_snowflake_wrapper
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.email import send_email

def email_fail_alert(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url

    error_msg = context.get('exception')
    clean_error = str(error_msg)[:200] if error_msg else "No specific error log found."

    subject = f"[Airflow] Task Failed: {dag_id}.{task_id}"

    html_content = f"""
    <h3 style="color:red;">Airflow Task Failure</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Error:</strong> {clean_error}</p>
    <p><strong>Logs:</strong> <a href="{log_url}">Click here to debug</a></p>
    """

    send_email(
        to=["skyfortcafe@yahoo.com"], 
        subject=subject,
        html_content=html_content
    )

dbt_key = Variable.get("dbt_api", deserialize_json=True)
ACCOUNT_ID = dbt_key["DBT_ACCOUNT_ID"]
JOB_ID = dbt_key["DBT_JOB_ID"]
API_TOKEN = dbt_key["DBT_API_TOKEN"]

HEADERS = {
    "Authorization": f"Token {API_TOKEN}",
    "Content-Type": "application/json"
}

BASE_URL = "https://nm130.us1.dbt.com/api/v2"


def trigger_dbt_job(**context):
    url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"

    response = requests.post(url, headers=HEADERS, json={})

    if response.status_code != 200:
        raise AirflowException(f"Failed to trigger dbt job: {response.text}")

    run_id = response.json()["data"]["id"]

    # Push to XCom
    context["ti"].xcom_push(key="run_id", value=run_id)


def check_dbt_job_status(**context):
    run_id = context["ti"].xcom_pull(
        task_ids="trigger_dbt_job",
        key="run_id"
    )

    url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/runs/{run_id}/"

    while True:
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            raise AirflowException(f"Error checking status: {response.text}")

        status = response.json()["data"]["status"]

        # dbt status codes:
        # 1 = queued
        # 2 = running
        # 3 = success
        # 10 = error
        # 20 = cancelled

        if status == 3:
            print("dbt job succeeded")
            break
        elif status in [10, 20]:
            raise AirflowException(f"dbt job failed with status: {status}")
        else:
            print(f"Job still running... status={status}")
            time.sleep(30)


default_args = {
    "owner": "Kabir Olawale Mohammed",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": email_fail_alert
}

with DAG(
    dag_id="launchpad_capstone",
    default_args=default_args,
    description="Launchpad capstone project",
    schedule="0 1 * * *",
    start_date=datetime(2025, 12, 1, 0, tz="UTC"),
    template_searchpath="/opt/airflow/dags/launchpad_capstone/include"
) as dag:

    # Download raw files from Launchpad S3
    download_raw_files_from_s3 = PythonOperator(
        task_id="download_raw_files_from_s3",
        python_callable=download_wrapper_s3,
    )

    # Download sales details from Postgres
    download_sales_details = PythonOperator(
        task_id="download_sales_details",
        python_callable=extract_transactions_details_wrapper,
    )

    # Download static data files (stores details) from Google Sheets
    download_stores_details = PythonOperator(
        task_id="download_stores_details",
        python_callable=extract_stores_details_wrapper,
    )

    # Upload processed files to capstone S3
    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=send_to_S3_wrapper,
    )

    # Upload data to Snowflake
    upload_to_snowflake = PythonOperator(
        task_id="upload_to_snowflake",
        python_callable=s3_to_snowflake_wrapper,
    )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to=["kabirolawalemohammed@yahoo.com", "skyfortcafe@gmail.com"],
        subject="SupplyChain360 Pipeline Completed Successfully",
        html_content="""
        <h3> SupplyChain360 Pipeline Update</h3>
        <p>Hi Team,</p>
        <p>The Airflow pipeline ran successfully</p>
        <p>The dbt mart Layer is ready for dashboard analysis.</p>
        <p>Kind Regards,</p>
        <p>Engineering Team</p>
        """,
        conn_id="smtp_conn"
    )
    
    dbt_trigger = PythonOperator(
        task_id="trigger_dbt_job",
        python_callable=trigger_dbt_job
    )

    dbt_monitor = PythonOperator(
        task_id="check_dbt_job_status",
        python_callable=check_dbt_job_status
    )

    download_raw_files_from_s3 >> download_sales_details >> download_stores_details >> upload_to_s3 >> upload_to_snowflake >> dbt_trigger >> dbt_monitor >> send_success_email