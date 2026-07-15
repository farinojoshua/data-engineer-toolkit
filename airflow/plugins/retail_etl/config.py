"""
Retail ETL – centralised configuration.
All path constants use the in-container /opt/airflow/data prefix.
"""
import os

RAW_DIR = "/opt/airflow/data/raw"
BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"
GOLD_DIR = "/opt/airflow/data/gold"
STATE_DIR = "/opt/airflow/data/state"

DB_CONN = "postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow"
GOLD_SCHEMA = "retail_datamart"


def get_unique_id() -> str:
    """Return the RETAIL_UNIQUE_ID Airflow Variable, falling back to the default."""
    try:
        from airflow.models import Variable
        return Variable.get("RETAIL_UNIQUE_ID", default_var="JoshuaFarino")
    except Exception:
        return "JoshuaFarino"
