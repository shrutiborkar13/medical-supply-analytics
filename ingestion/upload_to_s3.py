import pandas as pd
import boto3
import os
import io
from dotenv import load_dotenv

load_dotenv()

# AWS connection
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET = os.getenv("S3_BUCKET")

# Files to upload
files = {
    "orders":    "data/raw/olist_orders_dataset.csv",
    "payments":  "data/raw/olist_order_payments_dataset.csv",
    "items":     "data/raw/olist_order_items_dataset.csv",
    "fraud":     "data/raw/train_transaction.csv",
    "patients":  "data/raw/patients.csv",
    "encounters":"data/raw/encounters.csv",
}

def upload_to_s3(df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"✔ Uploaded: {key}")

# Main loop
for name, filepath in files.items():
    print(f"Processing {name}...")

    # fraud file is large - read in chunks
    if name == "fraud":
        df = pd.read_csv(filepath, nrows=50000)
    else:
        df = pd.read_csv(filepath)

    s3_key = f"raw/{name}/{name}.parquet"
    upload_to_s3(df, BUCKET, s3_key)

print("\nAll files uploaded to S3!")

