import os
import pandas as pd

def test_no_negative_sales():
    out_dir = os.path.join(os.path.dirname(__file__), "..", "out")
    # default to datasets if out doesn't exist yet
    fact_path = os.path.join(out_dir, "fact_sales.csv")
    if not os.path.exists(fact_path):
        fact_path = os.path.join(os.path.dirname(__file__), "..", "datasets", "sales_fact.csv")
    df = pd.read_csv(fact_path)
    assert (df['sales_amount'] >= 0).all(), "Negative sales_amount found"

def test_required_columns_present():
    out_dir = os.path.join(os.path.dirname(__file__), "..", "out")
    fact_path = os.path.join(out_dir, "fact_sales.csv")
    if not os.path.exists(fact_path):
        fact_path = os.path.join(os.path.dirname(__file__), "..", "datasets", "sales_fact.csv")
    df = pd.read_csv(fact_path)
    for col in ['order_date','customer_id','product_id','quantity','sales_amount']:
        assert col in df.columns, f"Missing required column: {col}"
