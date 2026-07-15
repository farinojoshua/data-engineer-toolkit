"""
Retail Medallion Pipeline DAG
Orchestrates Bronze -> Silver -> Gold ETL for the retail dataset.
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
    "retries": 1,
}

# ---------------------------------------------------------------------------
# Helper: resolve paths and unique_id at task-runtime so the plugin module
# is imported inside the worker process (avoids import-time side effects).
# ---------------------------------------------------------------------------

def _run_bronze():
    from retail_etl.config import RAW_DIR, BRONZE_DIR, STATE_DIR, get_unique_id
    from retail_etl.bronze import ingest_to_bronze

    unique_id = get_unique_id()
    new_files = ingest_to_bronze(RAW_DIR, BRONZE_DIR, STATE_DIR, unique_id)
    print(f"[DAG] Bronze complete. {len(new_files)} new files ingested.")
    return new_files


def _run_silver():
    from retail_etl.config import BRONZE_DIR, SILVER_DIR
    from retail_etl.silver import transform_to_silver

    tables = transform_to_silver(BRONZE_DIR, SILVER_DIR)
    print(f"[DAG] Silver complete. Tables processed: {tables}")
    return tables


def _run_gold():
    from retail_etl.config import SILVER_DIR, DB_CONN, GOLD_SCHEMA
    from retail_etl.gold import load_to_gold

    load_to_gold(SILVER_DIR, DB_CONN, GOLD_SCHEMA)
    print("[DAG] Gold complete.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="retail_medallion_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    description="Retail ETL: Bronze → Silver → Gold (star schema in PostgreSQL)",
    tags=["retail", "etl", "medallion"],
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_run_bronze,
        doc_md="""
## ingest_bronze

**Layer**: Bronze
**Purpose**: Scan the raw CSV directory for files that have not yet been ingested
and copy them to the bronze landing zone with metadata sidecar files.

**Incremental logic**: A JSON state file (`data/state/bronze_state.json`) tracks
every relative path that has already been copied.  Only new files are processed.

**Output**: Updated bronze layer at `/opt/airflow/data/bronze/{table}/`.
        """,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=_run_silver,
        doc_md="""
## transform_silver

**Layer**: Silver
**Purpose**: Read every CSV from the bronze layer, apply per-table data-quality
cleaners, and save the result as Parquet.

**DQ rules applied**:
- Strip whitespace from string fields
- Title-case city names
- Normalize dates to YYYY-MM-DD (multiple input formats accepted)
- Replace decimal-comma prices (e.g. "15,000" → 15000.0)
- Set empty email to NULL
- Remove rows with quantity ≤ 0
- Clip rating to 1–5
- Nullify delivered_date when it precedes shipped_date
- Deduplicate by primary key, keeping the last occurrence

**Output**: Parquet files at `/opt/airflow/data/silver/{table}/{table}.parquet`.
        """,
    )

    load_gold = PythonOperator(
        task_id="load_gold",
        python_callable=_run_gold,
        doc_md="""
## load_gold

**Layer**: Gold (Data Mart)
**Purpose**: Build and populate a star schema in the `retail_datamart` schema of
the Airflow PostgreSQL instance.

**Tables created/updated**:
- `dim_date` – Date dimension 2023-2027
- `dim_customer` – SCD-0 customer dimension
- `dim_product` – Product with denormalised category name
- `dim_store` – Store dimension
- `fact_sales` – Grain: one row per order_item

**Analysis views**:
- `vw_revenue_by_month`
- `vw_revenue_by_category`
- `vw_revenue_by_store`
- `vw_top_products`
- `vw_customer_segment_analysis`
- `vw_order_status_distribution`
- `vw_payment_method_analysis`

**Upsert strategy**: `INSERT … ON CONFLICT (unique_col) DO UPDATE SET …`
        """,
    )

    # Task dependencies
    ingest_bronze >> transform_silver >> load_gold
