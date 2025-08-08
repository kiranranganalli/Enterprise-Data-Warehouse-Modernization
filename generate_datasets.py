"""
Generates synthetic datasets for EDW modernization demos:
- datasets/customer_data.csv (10,000 rows)
- datasets/product_dim.csv   (5,000 rows)
- datasets/sales_fact.csv    (100,000 rows)
"""
import os
from pathlib import Path
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

BASE = Path(__file__).parent if "__file__" in globals() else Path.cwd()
OUT = BASE / "datasets"
OUT.mkdir(exist_ok=True)

random.seed(42)
np.random.seed(42)

regions = ["NA", "EU", "APAC", "LATAM"]

# Customers
n_customers = 10000
cust = pd.DataFrame({
    "customer_id": np.arange(1, n_customers+1),
    "email": [f"user{idx}@example.com" for idx in range(1, n_customers+1)],
    "region": np.random.choice(regions, n_customers),
    "is_vip": np.random.choice([0,1], n_customers, p=[0.9,0.1])
})

# Products
n_products = 5000
cats = ["Electronics","Home","Apparel","Grocery","Beauty","Books"]
prod = pd.DataFrame({
    "product_id": np.arange(1, n_products+1),
    "category": np.random.choice(cats, n_products),
    "unit_cost": np.round(np.random.uniform(1.0, 250.0, n_products), 2),
})

# Sales
n_facts = 100000
start = datetime(2022,1,1)

sales = []
for i in range(n_facts):
    d = start + timedelta(days=int(np.random.exponential(365)))
    qty = max(1, int(abs(np.random.normal(2, 1))))
    pid = np.random.randint(1, n_products+1)
    price = float(prod.loc[pid-1, "unit_cost"]) * np.random.uniform(1.2, 1.8)
    amt = round(qty * price, 2)
    sales.append({
        "order_id": i+1,
        "order_date": d.date().isoformat(),
        "customer_id": np.random.randint(1, n_customers+1),
        "product_id": pid,
        "quantity": qty,
        "sales_amount": amt,
    })

sales = pd.DataFrame(sales)

# Write files
cust.to_csv(OUT/"customer_data.csv", index=False)
prod.to_csv(OUT/"product_dim.csv", index=False)
sales.to_csv(OUT/"sales_fact.csv", index=False)
print("Datasets generated under ./datasets")
