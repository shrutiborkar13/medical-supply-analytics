import pandas as pd
import boto3
import io
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Neon connection
NEON_URL = "postgresql://neondb_owner:npg_NpQ4SulBe3nq@ep-frosty-field-amxfexd9-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(NEON_URL)

# S3 connection
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET = os.getenv("S3_BUCKET")

# Create staging schema
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    conn.commit()
    print("Schema created")

# Upload mart tables directly
mart_tables = {
    "staging.mart_order_fulfillment": "Read from local postgres",
    "staging.mart_shipment_delays": "Read from local postgres",
    "staging.mart_fraud_signals": "Read from local postgres"
}

# Read from local postgres and upload to Neon
local_engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

tables = [
    "mart_order_fulfillment",
    "mart_shipment_delays", 
    "mart_fraud_signals"
]

for table in tables:
    print(f"Uploading {table} to Neon...")
    df = pd.read_sql(f"SELECT * FROM staging.{table}", local_engine)
    df.to_sql(table, engine, schema="staging", if_exists="replace", index=False)
    print(f"✔ {table} uploaded — {len(df)} rows")

print("\nAll tables uploaded to Neon!")
