import pandas as pd
import os

files = [
    "data/raw/olist_orders_dataset.csv",
    "data/raw/olist_order_payments_dataset.csv",
    "data/raw/olist_order_items_dataset.csv",
    "data/raw/train_transaction.csv",
    "data/raw/patients.csv",
    "data/raw/encounters.csv",
]

for f in files:
    if os.path.exists(f):
        df = pd.read_csv(f, nrows=5)
        print(f"✔ {f.split('/')[-1]} — file exists and readable")
    else:
        print(f"✘ MISSING: {f}")

print("\nAll checks done!")