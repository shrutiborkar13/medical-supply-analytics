import subprocess
import sys
from prefect import flow, task

@task(name="upload_to_s3")
def upload_to_s3():
    print("Starting S3 upload...")
    subprocess.run(
        [sys.executable, "ingestion/upload_to_s3.py"],
        cwd="C:/Users/91704/medical-supply-pipeline",
        check=True
    )
    print("S3 upload done!")

@task(name="load_to_postgres")
def load_to_postgres():
    print("Loading to PostgreSQL...")
    subprocess.run(
        [sys.executable, "ingestion/load_to_postgres.py"],
        cwd="C:/Users/91704/medical-supply-pipeline",
        check=True
    )
    print("PostgreSQL load done!")

@task(name="run_dbt")
def run_dbt():
    print("Running dbt models...")
    subprocess.run(
        ["dbt", "run"],
        cwd="C:/Users/91704/medical-supply-pipeline/transform/medical_supply",
        check=True
    )
    print("dbt run done!")

@flow(name="medical-supply-pipeline")
def main_pipeline():
    upload_to_s3()
    load_to_postgres()
    run_dbt()

if __name__ == "__main__":
    main_pipeline()
    