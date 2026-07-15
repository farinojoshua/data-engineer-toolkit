"""Silver Layer: Data cleaning and standardisation."""
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Shared cleaning helpers
# ---------------------------------------------------------------------------

def _clean_price(val) -> Optional[float]:
    """
    Convert a price value to float.
    Handles decimal-comma notation (e.g. "15,000" → 15000.0)
    and strips currency symbols / extra whitespace.
    Returns None for invalid / non-positive values.
    """
    if pd.isna(val) or str(val).strip() == "":
        return None
    # Replace comma used as decimal separator with a dot
    cleaned = str(val).strip().replace(",", ".")
    # Remove everything except digits, dot and minus
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    # If multiple dots remain after cleaning, keep only the last one
    parts = cleaned.split(".")
    if len(parts) > 2:
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        result = float(cleaned)
        return result if result > 0 else None
    except ValueError:
        return None


def _normalize_date(val) -> Optional[str]:
    """
    Parse a date string in several common formats and return YYYY-MM-DD.
    Returns None when the value is empty or cannot be parsed.
    """
    if pd.isna(val) or str(val).strip() in ("", "nan", "None", "NaT"):
        return None
    s = str(val).strip()
    formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%d %B %Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    print(f"[Silver] Warning: could not parse date '{s}' – setting NULL")
    return None


def _strip(series: pd.Series) -> pd.Series:
    """Strip leading/trailing whitespace from a string series."""
    return series.astype(str).str.strip()


def _title(series: pd.Series) -> pd.Series:
    """Title-case a string series after stripping whitespace."""
    return series.astype(str).str.strip().str.title()


# ---------------------------------------------------------------------------
# Bronze reader
# ---------------------------------------------------------------------------

def _read_bronze_table(bronze_dir: str, table_name: str) -> Optional[pd.DataFrame]:
    """
    Read all CSV files in bronze/{table_name}/ and concatenate them into a
    single DataFrame.  All columns are read as strings to preserve raw values.
    """
    table_dir = Path(bronze_dir) / table_name
    if not table_dir.exists():
        return None

    csv_files = sorted(table_dir.glob("*.csv"))
    if not csv_files:
        return None

    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            df["_source_file"] = f.name
            dfs.append(df)
            print(f"[Silver]   Read {len(df)} rows from {f.name}")
        except Exception as exc:
            print(f"[Silver] Warning: could not read {f}: {exc}")

    if not dfs:
        return None

    combined = pd.concat(dfs, ignore_index=True)
    print(f"[Silver] {table_name}: {len(combined)} total rows from {len(dfs)} file(s)")
    return combined


# ---------------------------------------------------------------------------
# Per-table cleaners
# ---------------------------------------------------------------------------

def _clean_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["category_name"] = _strip(df["category_name"])
    df = df[_strip(df["category_id"]) != ""]
    return df.drop_duplicates(subset=["category_id"], keep="last").reset_index(drop=True)


def _clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["store_name"] = _strip(df["store_name"])
    df["city"] = _title(df["city"])
    df["opened_date"] = df["opened_date"].apply(_normalize_date)
    return df.drop_duplicates(subset=["store_id"], keep="last").reset_index(drop=True)


def _clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["product_name"] = _strip(df["product_name"])
    df["unit_price"] = df["unit_price"].apply(_clean_price)
    df["category_id"] = _strip(df["category_id"])
    # Remove rows with invalid price or missing category
    df = df[df["unit_price"].notna() & (df["unit_price"] > 0)]
    df = df[df["category_id"] != ""]
    return df.drop_duplicates(subset=["product_id"], keep="last").reset_index(drop=True)


def _clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["name"] = _strip(df["name"])
    df["city"] = _title(df["city"])
    df["phone"] = _strip(df["phone"])
    df["segment"] = _strip(df["segment"])
    # Normalise email: empty string → None
    df["email"] = _strip(df["email"])
    df["email"] = df["email"].replace({"": None, "nan": None, "None": None})
    df["join_date"] = df["join_date"].apply(_normalize_date)
    return df.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)


def _clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = df["order_date"].apply(_normalize_date)
    # Drop rows without a valid date – they're unusable for fact_sales
    df = df[df["order_date"].notna()]

    valid_statuses = {"pending", "completed", "cancelled", "processing", "shipped", "unknown"}
    df["status"] = _strip(df["status"]).str.lower()
    df["status"] = df["status"].where(df["status"].isin(valid_statuses), "unknown")

    df["payment_method"] = _strip(df["payment_method"]).replace({"": "unknown"})
    df["order_id"] = _strip(df["order_id"])
    df["customer_id"] = _strip(df["customer_id"])
    df["store_id"] = _strip(df["store_id"])

    df = df[df["order_id"] != ""]
    df = df[df["customer_id"] != ""]
    return df.drop_duplicates(subset=["order_id"], keep="last").reset_index(drop=True)


def _clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = df["unit_price"].apply(_clean_price)
    # Remove non-positive quantity or invalid price
    df = df[df["quantity"].notna() & (df["quantity"] > 0)]
    df = df[df["unit_price"].notna() & (df["unit_price"] > 0)]
    df["quantity"] = df["quantity"].astype(int)
    df["total_amount"] = df["quantity"] * df["unit_price"]
    return df.drop_duplicates(subset=["order_item_id"], keep="last").reset_index(drop=True)


def _clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["amount"] = df["amount"].apply(_clean_price)
    df = df[df["amount"].notna() & (df["amount"] > 0)]
    df["paid_at"] = df["paid_at"].apply(_normalize_date)

    valid_statuses = {"paid", "pending", "failed", "refunded"}
    df["status"] = _strip(df["status"]).str.lower()
    df["status"] = df["status"].where(df["status"].isin(valid_statuses), "unknown")
    df["method"] = _strip(df["method"])
    return df.drop_duplicates(subset=["payment_id"], keep="last").reset_index(drop=True)


def _clean_shipments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["shipped_date"] = df["shipped_date"].apply(_normalize_date)
    df["delivered_date"] = df["delivered_date"].apply(_normalize_date)

    # Nullify delivered_date when it precedes shipped_date
    mask = (
        df["delivered_date"].notna()
        & df["shipped_date"].notna()
        & (df["delivered_date"] < df["shipped_date"])
    )
    df.loc[mask, "delivered_date"] = None
    if mask.sum():
        print(f"[Silver] shipments: fixed {mask.sum()} delivered-before-shipped rows")

    df["courier"] = _strip(df["courier"]).replace({"": "Unknown"})
    df["order_id"] = _strip(df["order_id"])
    df = df[df["order_id"] != ""]
    return df.drop_duplicates(subset=["shipment_id"], keep="last").reset_index(drop=True)


def _clean_product_reviews(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").clip(1, 5)
    df = df[df["rating"].notna()]
    df["review_text"] = df["review_text"].fillna("").str.strip()
    df["review_date"] = df["review_date"].apply(_normalize_date)
    df["customer_id"] = _strip(df["customer_id"])
    df["product_id"] = _strip(df["product_id"])
    return df.drop_duplicates(subset=["review_id"], keep="last").reset_index(drop=True)


def _clean_promotions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["promo_code"] = _strip(df["promo_code"])
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").clip(0, 100)
    df["start_date"] = df["start_date"].apply(_normalize_date)
    df["end_date"] = df["end_date"].apply(_normalize_date)
    return df.drop_duplicates(subset=["promo_id"], keep="last").reset_index(drop=True)


def _clean_order_promotions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_id"] = _strip(df["order_id"])
    df["promo_id"] = _strip(df["promo_id"])
    df = df[df["order_id"] != ""]
    df = df[df["promo_id"] != ""]
    return df.drop_duplicates(subset=["order_id", "promo_id"], keep="last").reset_index(drop=True)


def _clean_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["supplier_name"] = _strip(df["supplier_name"])
    df["city"] = _title(df["city"]).replace({"": "Unknown"})
    return df.drop_duplicates(subset=["supplier_id"], keep="last").reset_index(drop=True)


def _clean_product_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cost_price"] = df["cost_price"].apply(_clean_price)
    df["product_id"] = _strip(df["product_id"])
    df["supplier_id"] = _strip(df["supplier_id"])
    return df.drop_duplicates(subset=["product_id", "supplier_id"], keep="last").reset_index(drop=True)


def _clean_employees(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["name"] = _strip(df["name"])
    df["role"] = _strip(df["role"])
    df["hire_date"] = df["hire_date"].apply(_normalize_date)
    df["store_id"] = _strip(df["store_id"])
    df = df[df["store_id"] != ""]
    return df.drop_duplicates(subset=["employee_id"], keep="last").reset_index(drop=True)


def _clean_support_tickets(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["issue_type"] = _strip(df["issue_type"])
    df["status"] = _strip(df["status"]).str.lower()
    df["created_at"] = df["created_at"].apply(_normalize_date)
    df["customer_id"] = _strip(df["customer_id"])
    df["order_id"] = _strip(df["order_id"])
    return df.drop_duplicates(subset=["ticket_id"], keep="last").reset_index(drop=True)


def _clean_loyalty_points(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["points_earned"] = pd.to_numeric(df["points_earned"], errors="coerce")
    df["points_redeemed"] = pd.to_numeric(df["points_redeemed"], errors="coerce").fillna(0)
    df = df[df["points_earned"].notna() & (df["points_earned"] >= 0)]
    df["customer_id"] = _strip(df["customer_id"])
    df["order_id"] = _strip(df["order_id"])
    return df.drop_duplicates(subset=["loyalty_id"], keep="last").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Cleaner registry
# ---------------------------------------------------------------------------

CLEANERS = {
    "categories": _clean_categories,
    "stores": _clean_stores,
    "products": _clean_products,
    "customers": _clean_customers,
    "orders": _clean_orders,
    "order_items": _clean_order_items,
    "payments": _clean_payments,
    "shipments": _clean_shipments,
    "product_reviews": _clean_product_reviews,
    "promotions": _clean_promotions,
    "order_promotions": _clean_order_promotions,
    "suppliers": _clean_suppliers,
    "product_suppliers": _clean_product_suppliers,
    "employees": _clean_employees,
    "support_tickets": _clean_support_tickets,
    "loyalty_points": _clean_loyalty_points,
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def transform_to_silver(bronze_dir: str, silver_dir: str) -> list:
    """
    Read all bronze CSV files, apply per-table data-quality cleaners, and
    save the results as Parquet in the silver layer.

    Always rebuilds silver fully from all available bronze files (merge all
    CSVs per table, deduplicate by PK keeping the last occurrence).

    Parameters
    ----------
    bronze_dir : path to the bronze landing zone
    silver_dir : path to the silver output directory

    Returns
    -------
    list of table names successfully processed
    """
    bronze_base = Path(bronze_dir)
    silver_base = Path(silver_dir)

    if not bronze_base.exists():
        print("[Silver] Bronze directory not found. Run bronze ingestion first.")
        return []

    processed_at = datetime.now().isoformat()
    tables_processed = []

    for table_dir in sorted(bronze_base.iterdir()):
        if not table_dir.is_dir():
            continue

        table_name = table_dir.name
        print(f"\n[Silver] Processing table: {table_name}")

        df = _read_bronze_table(bronze_dir, table_name)
        if df is None or df.empty:
            print(f"[Silver] {table_name}: no data found – skipping.")
            continue

        raw_count = len(df)

        # Apply table-specific cleaner (or generic dedup if unknown)
        if table_name in CLEANERS:
            try:
                df = CLEANERS[table_name](df)
            except Exception as exc:
                print(
                    f"[Silver] Warning: cleaner for {table_name} raised {exc}. "
                    "Falling back to generic dedup."
                )
                df = df.drop_duplicates().reset_index(drop=True)
        else:
            print(f"[Silver] {table_name}: no cleaner registered – applying generic dedup.")
            df = df.drop_duplicates().reset_index(drop=True)

        # Add silver metadata column
        df["_silver_processed_at"] = processed_at

        # Persist as Parquet
        out_dir = silver_base / table_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{table_name}.parquet"
        df.to_parquet(out_file, index=False, engine="pyarrow")

        tables_processed.append(table_name)
        print(
            f"[Silver] {table_name}: {raw_count} raw rows → {len(df)} cleaned rows "
            f"→ saved to {out_file}"
        )

    print(f"\n[Silver] Done. {len(tables_processed)} table(s) processed: {tables_processed}")
    return tables_processed
