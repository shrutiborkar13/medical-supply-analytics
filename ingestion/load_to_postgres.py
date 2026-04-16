import pandas as pd
import boto3
import io
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

BUCKET = os.getenv("S3_BUCKET")

tables = ["orders", "payments", "items", "fraud", "patients", "encounters"]

# Drop views first so raw tables can be replaced
with engine.connect() as conn:
    conn.execute(text("DROP VIEW IF EXISTS staging.mart_shipment_delays CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS staging.mart_order_fulfillment CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS staging.mart_fraud_signals CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS staging.stg_orders CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS staging.stg_payments CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS staging.stg_fraud CASCADE;"))
    conn.commit()
    print("Views dropped successfully")

for table in tables:
    print(f"Loading {table}...")
    key = f"raw/{table}/{table}.parquet"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    df.to_sql(f"raw_{table}", engine, if_exists="replace", index=False)
    print(f"✔ {table} loaded — {len(df)} rows")

print("\nAll tables loaded into PostgreSQL!")