from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    'owner': 'shruti',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_upload_to_s3():
    subprocess.run(
        [sys.executable, "ingestion/upload_to_s3.py"],
        cwd="C:/Users/91704/medical-supply-pipeline",
        check=True
    )

def run_load_to_postgres():
    subprocess.run(
        [sys.executable, "ingestion/load_to_postgres.py"],
        cwd="C:/Users/91704/medical-supply-pipeline",
        check=True
    )

def run_dbt():
    subprocess.run(
        ["dbt", "run"],
        cwd="C:/Users/91704/medical-supply-pipeline/transform/medical_supply",
        check=True
    )

with DAG(
    dag_id="medical_supply_pipeline",
    default_args=default_args,
    description="End to end medical supply pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=run_upload_to_s3,
    )

    task2 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load_to_postgres,
    )

    task3 = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    task1 >> task2 >> task3