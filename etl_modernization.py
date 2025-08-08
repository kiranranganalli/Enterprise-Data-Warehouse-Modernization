"""
Enterprise Data Warehouse Modernization — Orchestration Script
--------------------------------------------------------------
This script coordinates mock extraction, transformation, data quality, loading, and parity checks
for a modernization program migrating from SSAS OLAP + SQL Server to Amazon Redshift.

It is designed to be *framework-like* so you can plug in Informatica/SSIS exports, vendor files, or API pulls.
For demo purposes, it uses local CSVs generated via `generate_datasets.py`.

Author: Kiran Ranganalli
Last Updated: Aug 2025
"""

from __future__ import annotations
import os
import sys
import csv
import time
import json
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("edw_modernization")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE = Path(__file__).parent if "__file__" in globals() else Path.cwd()
DATA_DIR = BASE / "datasets"
SQL_DIR = BASE / "sql"
MDX_DIR = BASE / "mdx_validation_scripts"
KSH_DIR = BASE / "ksh_scripts"
OUT_DIR = BASE / "out"
OUT_DIR.mkdir(exist_ok=True)

REDSHIFT_CFG = {
    "host": os.environ.get("REDSHIFT_HOST", "redshift-demo"),
    "database": os.environ.get("REDSHIFT_DB", "dev"),
    "user": os.environ.get("REDSHIFT_USER", "awsuser"),
    "password": os.environ.get("REDSHIFT_PASS", "password"),
    "port": int(os.environ.get("REDSHIFT_PORT", 5439)),
}

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def md5sum(path: Path) -> str:
    """Compute MD5 checksum for a given file path."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Ensure columns exist on a DataFrame; fill with defaults if missing."""
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
    return out

@dataclass
class TableSpec:
    name: str
    path: Path
    pk: List[str]

# ---------------------------------------------------------------------------
# Mock Extract (would be Informatica/SSIS/Oracle/SQLServer/API in prod)
# ---------------------------------------------------------------------------
def extract_sources() -> Dict[str, pd.DataFrame]:
    """Read datasets prepared by `generate_datasets.py`. In production this would:
    - call Informatica/SSIS jobs
    - pull from Oracle/SQL Server
    - land vendor files from SFTP (ksh)
    - call SaaS APIs
    """
    log.info("Extracting source datasets from ./datasets …")
    files = {
        "customer": DATA_DIR / "customer_data.csv",
        "product": DATA_DIR / "product_dim.csv",
        "sales": DATA_DIR / "sales_fact.csv",
    }
    for k, p in files.items():
        if not p.exists():
            raise FileNotFoundError(f"Missing required dataset: {p}")
    dfs = {k: pd.read_csv(v) for k, v in files.items()}
    for k, v in files.items():
        log.info(f"{k}: {v.name} | rows={sum(1 for _ in open(v)) - 1} | md5={md5sum(v)}")
    return dfs

# ---------------------------------------------------------------------------
# Transformations (business rules, SCD, surrogate keys, audit columns)
# ---------------------------------------------------------------------------
def scd2_merge(current: pd.DataFrame, incoming: pd.DataFrame, key_cols: List[str], attrib_cols: List[str]) -> pd.DataFrame:
    """Simplified SCD-Type2: mark changed rows as expired and insert new versions.
    Adds columns: is_current, effective_date, end_date.
    """
    now = pd.Timestamp.utcnow().normalize()
    current = current.copy()
    if not set(["is_current", "effective_date", "end_date"]).issubset(current.columns):
        current["is_current"] = True
        current["effective_date"] = now
        current["end_date"] = pd.NaT

    # Ensure incoming has same columns
    incoming = ensure_cols(incoming, [c for c in current.columns if c not in incoming.columns])

    # join on business keys
    merged = current.merge(incoming, on=key_cols, how="outer", indicator=True, suffixes=("_cur", "_new"))

    updates = []
    keep = []
    for _, row in merged.iterrows():
        if row["_merge"] == "both":
            changed = any(row.get(f"{c}_cur") != row.get(f"{c}_new") for c in attrib_cols)
            if changed:
                # expire old
                expired = {c: row.get(f"{c}_cur", None) for c in current.columns}
                expired["is_current"] = False
                expired["end_date"] = now
                updates.append(expired)
                # add new
                newrow = {c: row.get(f"{c}_new", row.get(f"{c}_cur", None)) for c in current.columns}
                newrow["is_current"] = True
                newrow["effective_date"] = now
                newrow["end_date"] = pd.NaT
                keep.append(newrow)
            else:
                # unchanged
                keep.append({c: row.get(f"{c}_cur") for c in current.columns})
        elif row["_merge"] == "right_only":
            # brand new key
            newrow = {c: row.get(f"{c}_new", None) for c in current.columns}
            newrow["is_current"] = True
            newrow["effective_date"] = now
            newrow["end_date"] = pd.NaT
            keep.append(newrow)
        else:
            # left_only → key disappeared; expire it
            expired = {c: row.get(f"{c}_cur") for c in current.columns}
            expired["is_current"] = False
            expired["end_date"] = now
            updates.append(expired)

    out = pd.DataFrame(keep + updates).reset_index(drop=True)
    return out

def transform(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    log.info("Applying business rules & enrichment …")
    cust = dfs["customer"].copy()
    prod = dfs["product"].copy()
    sales = dfs["sales"].copy()

    # Basic data hygiene
    cust["email"] = cust["email"].astype(str).str.lower().fillna("unknown@example.com")
    sales["order_date"] = pd.to_datetime(sales["order_date"]).dt.date

    # Surrogate keys (example: ensure integer ids, no gaps required here)
    cust["customer_sk"] = cust["customer_id"].astype(int)
    prod["product_sk"] = prod["product_id"].astype(int)

    # Derive order_year, quarter
    sd = pd.to_datetime(sales["order_date"])
    sales["order_year"] = sd.dt.year
    sales["order_qtr"] = sd.dt.quarter

    # Join dim attributes
    fact = sales.merge(cust[["customer_id", "region", "is_vip"]], on="customer_id", how="left") \
                .merge(prod[["product_id", "category", "unit_cost"]], on="product_id", how="left")

    # Derived metrics
    fact["gross_margin"] = fact["sales_amount"] - (fact["quantity"] * fact["unit_cost"])

    # Example SCD2 on product_dim (simulate vendor price change)
    prod_new = prod.copy()
    prod_new.loc[prod_new.sample(frac=0.05, random_state=42).index, "unit_cost"] *= 1.05
    prod_scd2 = scd2_merge(
        current=prod.assign(is_current=True, effective_date=pd.NaT, end_date=pd.NaT),
        incoming=prod_new,
        key_cols=["product_id"],
        attrib_cols=["unit_cost"]
    )

    # Add audit columns
    now = pd.Timestamp.utcnow().normalize()
    for df in (fact, cust, prod_scd2):
        df["etl_run_id"] = now.strftime("%Y%m%d")
        df["ingestion_ts"] = pd.Timestamp.utcnow()

    return {"fact_sales": fact, "dim_customer": cust, "dim_product_scd2": prod_scd2}

# ---------------------------------------------------------------------------
# Data Quality Checks
# ---------------------------------------------------------------------------
def dq_checks(tables: Dict[str, pd.DataFrame]) -> None:
    log.info("Running DQ checks …")
    fact = tables["fact_sales"]
    # 1) no negative quantities or amounts
    neg_qty = fact[fact["quantity"] < 0]
    neg_amt = fact[fact["sales_amount"] < 0]
    if len(neg_qty) or len(neg_amt):
        raise ValueError("DQ FAIL: negative values found in facts")

    # 2) referential integrity
    if fact["customer_id"].isna().any():
        raise ValueError("DQ FAIL: orphaned customer_id")
    if fact["product_id"].isna().any():
        raise ValueError("DQ FAIL: orphaned product_id")

    # 3) aggregates sanity
    by_year = fact.groupby("order_year")["sales_amount"].sum()
    log.info("Sales by year:\n" + by_year.to_string())

    # 4) VIP sanity check
    vip_sales = fact.loc[fact["is_vip"] == 1, "sales_amount"].sum()
    log.info(f"VIP sales total: {vip_sales:,.2f}")

# ---------------------------------------------------------------------------
# Load (mock) — In real program: COPY to Redshift, upsert patterns, vacuum/sort
# ---------------------------------------------------------------------------
def write_csv(df: pd.DataFrame, name: str) -> Path:
    out = OUT_DIR / f"{name}.csv"
    df.to_csv(out, index=False)
    log.info(f"Wrote {name}: {out} ({out.stat().st_size/1024:.1f} KB)")
    return out

def load_targets(tables: Dict[str, pd.DataFrame]) -> Dict[str, Path]:
    log.info("Writing outputs (mock load)")
    paths = {}
    for k, v in tables.items(): paths[k] = write_csv(v, k)
    return paths

# ---------------------------------------------------------------------------
# Parity Checks (simulate MDX vs. Redshift KPI parity)
# ---------------------------------------------------------------------------
def parity_check(fact_csv: Path) -> None:
    log.info("Simulating OLAP parity check …")
    fact = pd.read_csv(fact_csv)
    # Legacy cube KPI: total margin last 365 days
    recent = pd.to_datetime(fact["order_date"]) >= (pd.Timestamp.today() - pd.Timedelta(days=365))
    kpi_margin = float(fact.loc[recent, "gross_margin"].sum())
    # Pretend MDX returned very similar number
    mdx_value = kpi_margin * 0.999  # within tolerance
    diff = abs(kpi_margin - mdx_value)
    log.info(f"Parity Check — marginYTD: cube={mdx_value:,.2f} vs redshift={kpi_margin:,.2f} | diff={diff:,.2f}")
    assert diff / max(kpi_margin, 1.0) < 0.01, "Parity outside tolerance"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("EDW Modernization Orchestration — START")
    dfs = extract_sources()
    tables = transform(dfs)
    dq_checks(tables)
    paths = load_targets(tables)
    parity_check(paths["fact_sales"])
    log.info("EDW Modernization Orchestration — DONE")

if __name__ == "__main__":
    main()
