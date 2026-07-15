"""Gold Layer: Star schema loader for the retail data mart."""
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Schema / table DDL
# ---------------------------------------------------------------------------

_DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {schema};"

_DDL_DIM_DATE = """
CREATE TABLE IF NOT EXISTS {schema}.dim_date (
    date_key    INT         PRIMARY KEY,
    full_date   DATE        NOT NULL,
    year        SMALLINT    NOT NULL,
    quarter     SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    month_name  VARCHAR(10) NOT NULL,
    day         SMALLINT    NOT NULL,
    day_of_week SMALLINT    NOT NULL,
    day_name    VARCHAR(10) NOT NULL,
    is_weekend  BOOLEAN     NOT NULL
);
"""

_DDL_DIM_CUSTOMER = """
CREATE TABLE IF NOT EXISTS {schema}.dim_customer (
    customer_key SERIAL      PRIMARY KEY,
    customer_id  VARCHAR(50) UNIQUE NOT NULL,
    name         VARCHAR(200),
    email        VARCHAR(200),
    city         VARCHAR(100),
    segment      VARCHAR(50),
    join_date    DATE
);
"""

_DDL_DIM_PRODUCT = """
CREATE TABLE IF NOT EXISTS {schema}.dim_product (
    product_key   SERIAL      PRIMARY KEY,
    product_id    VARCHAR(50) UNIQUE NOT NULL,
    product_name  VARCHAR(300),
    category_name VARCHAR(100),
    unit_price    NUMERIC(14,2)
);
"""

_DDL_DIM_STORE = """
CREATE TABLE IF NOT EXISTS {schema}.dim_store (
    store_key   SERIAL      PRIMARY KEY,
    store_id    VARCHAR(50) UNIQUE NOT NULL,
    store_name  VARCHAR(200),
    city        VARCHAR(100),
    opened_date DATE
);
"""

_DDL_FACT_SALES = """
CREATE TABLE IF NOT EXISTS {schema}.fact_sales (
    fact_id        SERIAL        PRIMARY KEY,
    order_item_id  VARCHAR(50)   UNIQUE NOT NULL,
    order_id       VARCHAR(50)   NOT NULL,
    customer_key   INT           REFERENCES {schema}.dim_customer(customer_key),
    product_key    INT           REFERENCES {schema}.dim_product(product_key),
    store_key      INT           REFERENCES {schema}.dim_store(store_key),
    date_key       INT           REFERENCES {schema}.dim_date(date_key),
    quantity       INT,
    unit_price     NUMERIC(14,2),
    total_amount   NUMERIC(14,2),
    payment_method VARCHAR(50),
    order_status   VARCHAR(30)
);
"""

_ALL_DDL = [
    _DDL_SCHEMA,
    _DDL_DIM_DATE,
    _DDL_DIM_CUSTOMER,
    _DDL_DIM_PRODUCT,
    _DDL_DIM_STORE,
    _DDL_FACT_SALES,
]

# ---------------------------------------------------------------------------
# Analysis views SQL
# ---------------------------------------------------------------------------

_VIEWS_SQL = [
    (
        "vw_revenue_by_month",
        """
        SELECT
            dd.year,
            dd.month,
            dd.month_name,
            COUNT(DISTINCT fs.order_id) AS total_orders,
            SUM(fs.total_amount)        AS total_revenue,
            AVG(fs.total_amount)        AS avg_item_revenue
        FROM {schema}.fact_sales fs
        JOIN {schema}.dim_date    dd ON fs.date_key = dd.date_key
        GROUP BY dd.year, dd.month, dd.month_name
        ORDER BY dd.year, dd.month
        """,
    ),
    (
        "vw_revenue_by_category",
        """
        SELECT
            dp.category_name,
            COUNT(DISTINCT fs.order_id) AS total_orders,
            SUM(fs.quantity)            AS total_items_sold,
            SUM(fs.total_amount)        AS total_revenue,
            AVG(fs.unit_price)          AS avg_price
        FROM {schema}.fact_sales  fs
        JOIN {schema}.dim_product dp ON fs.product_key = dp.product_key
        GROUP BY dp.category_name
        ORDER BY total_revenue DESC
        """,
    ),
    (
        "vw_revenue_by_store",
        """
        SELECT
            ds.store_name,
            ds.city,
            COUNT(DISTINCT fs.order_id) AS total_orders,
            SUM(fs.total_amount)        AS total_revenue
        FROM {schema}.fact_sales fs
        JOIN {schema}.dim_store  ds ON fs.store_key = ds.store_key
        GROUP BY ds.store_name, ds.city
        ORDER BY total_revenue DESC
        """,
    ),
    (
        "vw_top_products",
        """
        SELECT
            dp.product_id,
            dp.product_name,
            dp.category_name,
            SUM(fs.quantity)    AS total_qty_sold,
            SUM(fs.total_amount) AS total_revenue,
            AVG(fs.unit_price)   AS avg_price
        FROM {schema}.fact_sales  fs
        JOIN {schema}.dim_product dp ON fs.product_key = dp.product_key
        GROUP BY dp.product_id, dp.product_name, dp.category_name
        ORDER BY total_revenue DESC
        LIMIT 20
        """,
    ),
    (
        "vw_customer_segment_analysis",
        """
        SELECT
            dc.segment,
            COUNT(DISTINCT fs.customer_key) AS total_customers,
            COUNT(DISTINCT fs.order_id)     AS total_orders,
            SUM(fs.total_amount)            AS total_revenue,
            AVG(fs.total_amount)            AS avg_order_value
        FROM {schema}.fact_sales    fs
        JOIN {schema}.dim_customer dc ON fs.customer_key = dc.customer_key
        GROUP BY dc.segment
        ORDER BY total_revenue DESC
        """,
    ),
    (
        "vw_order_status_distribution",
        """
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(total_amount)        AS total_amount
        FROM {schema}.fact_sales
        GROUP BY order_status
        """,
    ),
    (
        "vw_payment_method_analysis",
        """
        SELECT
            payment_method,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(total_amount)        AS total_amount,
            ROUND(
                100.0 * COUNT(DISTINCT order_id)
                / NULLIF(SUM(COUNT(DISTINCT order_id)) OVER (), 0),
                2
            ) AS pct_orders
        FROM {schema}.fact_sales
        GROUP BY payment_method
        ORDER BY order_count DESC
        """,
    ),
]


# ---------------------------------------------------------------------------
# Schema + table creation
# ---------------------------------------------------------------------------

def create_schema_and_tables(engine, schema: str) -> None:
    """Create the retail_datamart schema and all dimension / fact tables."""
    print(f"[Gold] Creating schema '{schema}' and tables …")
    with engine.begin() as conn:
        for ddl in _ALL_DDL:
            conn.execute(text(ddl.format(schema=schema)))
    print("[Gold] Schema and tables ready.")


# ---------------------------------------------------------------------------
# dim_date population
# ---------------------------------------------------------------------------

def populate_dim_date(engine, schema: str,
                      start_year: int = 2023, end_year: int = 2027) -> None:
    """
    Populate dim_date for [start_year, end_year] if not already populated.
    Uses INSERT … ON CONFLICT (date_key) DO NOTHING for idempotency.
    """
    print(f"[Gold] Populating dim_date ({start_year}–{end_year}) …")
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    day_names = [
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday",
    ]

    rows = []
    current = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    while current <= end:
        date_key = int(current.strftime("%Y%m%d"))
        quarter = (current.month - 1) // 3 + 1
        dow = current.weekday()  # 0=Mon … 6=Sun
        rows.append(
            (
                date_key,
                current,
                current.year,
                quarter,
                current.month,
                month_names[current.month - 1],
                current.day,
                dow + 1,           # 1=Mon … 7=Sun
                day_names[dow],
                dow >= 5,          # Saturday or Sunday
            )
        )
        current += timedelta(days=1)

    sql = f"""
        INSERT INTO {schema}.dim_date
            (date_key, full_date, year, quarter, month, month_name,
             day, day_of_week, day_name, is_weekend)
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING
    """
    conn_str = _psycopg2_conn_str(engine)
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

    print(f"[Gold] dim_date: {len(rows)} date rows inserted/skipped.")


# ---------------------------------------------------------------------------
# Parquet reader helper
# ---------------------------------------------------------------------------

def _read_silver(silver_dir: str, table_name: str) -> Optional[pd.DataFrame]:
    """Return the silver parquet for *table_name*, or None if absent."""
    pq_file = Path(silver_dir) / table_name / f"{table_name}.parquet"
    if not pq_file.exists():
        print(f"[Gold] Silver parquet not found for '{table_name}' – skipping.")
        return None
    df = pd.read_parquet(pq_file, engine="pyarrow")
    print(f"[Gold] Loaded {len(df)} rows from silver/{table_name}")
    return df


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def upsert_dim_customer(engine, schema: str, silver_dir: str) -> None:
    """Upsert customers from silver into dim_customer."""
    df = _read_silver(silver_dir, "customers")
    if df is None or df.empty:
        return

    # Only the columns we need
    cols = ["customer_id", "name", "email", "city", "segment", "join_date"]
    df = df[[c for c in cols if c in df.columns]].copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # Coerce join_date → date
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce").dt.date

    sql = f"""
        INSERT INTO {schema}.dim_customer
            (customer_id, name, email, city, segment, join_date)
        VALUES (%(customer_id)s, %(name)s, %(email)s, %(city)s,
                %(segment)s, %(join_date)s)
        ON CONFLICT (customer_id) DO UPDATE SET
            name      = EXCLUDED.name,
            email     = EXCLUDED.email,
            city      = EXCLUDED.city,
            segment   = EXCLUDED.segment,
            join_date = EXCLUDED.join_date
    """
    _bulk_upsert(engine, sql, df)
    print(f"[Gold] dim_customer: upserted {len(df)} rows.")


def upsert_dim_product(engine, schema: str, silver_dir: str) -> None:
    """
    Upsert products into dim_product.
    Joins with the categories parquet to denormalise category_name.
    """
    products = _read_silver(silver_dir, "products")
    if products is None or products.empty:
        return

    categories = _read_silver(silver_dir, "categories")
    if categories is not None and not categories.empty:
        products = products.merge(
            categories[["category_id", "category_name"]],
            on="category_id",
            how="left",
        )
    else:
        products["category_name"] = None

    cols = ["product_id", "product_name", "category_name", "unit_price"]
    df = products[[c for c in cols if c in products.columns]].copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    sql = f"""
        INSERT INTO {schema}.dim_product
            (product_id, product_name, category_name, unit_price)
        VALUES (%(product_id)s, %(product_name)s, %(category_name)s, %(unit_price)s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name  = EXCLUDED.product_name,
            category_name = EXCLUDED.category_name,
            unit_price    = EXCLUDED.unit_price
    """
    _bulk_upsert(engine, sql, df)
    print(f"[Gold] dim_product: upserted {len(df)} rows.")


def upsert_dim_store(engine, schema: str, silver_dir: str) -> None:
    """Upsert stores from silver into dim_store."""
    df = _read_silver(silver_dir, "stores")
    if df is None or df.empty:
        return

    cols = ["store_id", "store_name", "city", "opened_date"]
    df = df[[c for c in cols if c in df.columns]].copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df["opened_date"] = pd.to_datetime(df["opened_date"], errors="coerce").dt.date

    sql = f"""
        INSERT INTO {schema}.dim_store
            (store_id, store_name, city, opened_date)
        VALUES (%(store_id)s, %(store_name)s, %(city)s, %(opened_date)s)
        ON CONFLICT (store_id) DO UPDATE SET
            store_name  = EXCLUDED.store_name,
            city        = EXCLUDED.city,
            opened_date = EXCLUDED.opened_date
    """
    _bulk_upsert(engine, sql, df)
    print(f"[Gold] dim_store: upserted {len(df)} rows.")


# ---------------------------------------------------------------------------
# Fact table loader
# ---------------------------------------------------------------------------

def upsert_fact_sales(engine, schema: str, silver_dir: str) -> None:
    """
    Build fact_sales by joining order_items with orders, then looking up
    dimension surrogate keys.
    """
    order_items = _read_silver(silver_dir, "order_items")
    if order_items is None or order_items.empty:
        print("[Gold] No order_items data – fact_sales skipped.")
        return

    orders = _read_silver(silver_dir, "orders")
    if orders is None or orders.empty:
        print("[Gold] No orders data – fact_sales skipped.")
        return

    # Join order_items ← orders
    fact = order_items.merge(
        orders[["order_id", "customer_id", "store_id", "order_date",
                "payment_method", "status"]],
        on="order_id",
        how="inner",
    )

    if fact.empty:
        print("[Gold] fact join produced 0 rows – nothing to load.")
        return

    # --- Look up dimension surrogate keys ---
    # dim_customer keys
    customer_keys = _fetch_key_map(engine, schema, "dim_customer", "customer_id", "customer_key")
    fact["customer_key"] = fact["customer_id"].map(customer_keys)

    # dim_product keys
    product_keys = _fetch_key_map(engine, schema, "dim_product", "product_id", "product_key")
    fact["product_key"] = fact["product_id"].map(product_keys)

    # dim_store keys
    store_keys = _fetch_key_map(engine, schema, "dim_store", "store_id", "store_key")
    fact["store_key"] = fact["store_id"].map(store_keys)

    # dim_date keys  (date_key = int YYYYMMDD)
    fact["order_date"] = pd.to_datetime(fact["order_date"], errors="coerce")
    fact["date_key"] = fact["order_date"].dt.strftime("%Y%m%d").astype("Int64")

    # Numeric columns
    fact["quantity"] = pd.to_numeric(fact["quantity"], errors="coerce").astype("Int64")
    # unit_price may already be numeric from silver
    fact["unit_price"] = pd.to_numeric(fact["unit_price"], errors="coerce")
    fact["total_amount"] = pd.to_numeric(fact["total_amount"], errors="coerce")

    # Fallback: compute total_amount if missing
    mask = fact["total_amount"].isna() & fact["quantity"].notna() & fact["unit_price"].notna()
    fact.loc[mask, "total_amount"] = (
        fact.loc[mask, "quantity"] * fact.loc[mask, "unit_price"]
    )

    # Prepare final DataFrame for upsert
    cols = [
        "order_item_id", "order_id", "customer_key", "product_key",
        "store_key", "date_key", "quantity", "unit_price",
        "total_amount", "payment_method", "status",
    ]
    df = fact[[c for c in cols if c in fact.columns]].copy()
    df = df.rename(columns={"status": "order_status"})

    # Drop rows missing the business key
    df = df[df["order_item_id"].notna() & (df["order_item_id"].astype(str).str.strip() != "")]

    # Convert pandas NA to Python None for psycopg2
    df = df.where(pd.notnull(df), None)

    sql = f"""
        INSERT INTO {schema}.fact_sales
            (order_item_id, order_id, customer_key, product_key,
             store_key, date_key, quantity, unit_price,
             total_amount, payment_method, order_status)
        VALUES (%(order_item_id)s, %(order_id)s, %(customer_key)s, %(product_key)s,
                %(store_key)s, %(date_key)s, %(quantity)s, %(unit_price)s,
                %(total_amount)s, %(payment_method)s, %(order_status)s)
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id       = EXCLUDED.order_id,
            customer_key   = EXCLUDED.customer_key,
            product_key    = EXCLUDED.product_key,
            store_key      = EXCLUDED.store_key,
            date_key       = EXCLUDED.date_key,
            quantity       = EXCLUDED.quantity,
            unit_price     = EXCLUDED.unit_price,
            total_amount   = EXCLUDED.total_amount,
            payment_method = EXCLUDED.payment_method,
            order_status   = EXCLUDED.order_status
    """
    _bulk_upsert(engine, sql, df)
    print(f"[Gold] fact_sales: upserted {len(df)} rows.")


# ---------------------------------------------------------------------------
# Analysis views
# ---------------------------------------------------------------------------

def create_analysis_views(engine, schema: str) -> None:
    """Create or replace all analysis views in the gold schema."""
    print("[Gold] Creating analysis views …")
    with engine.begin() as conn:
        for view_name, view_sql in _VIEWS_SQL:
            full_name = f"{schema}.{view_name}"
            ddl = (
                f"CREATE OR REPLACE VIEW {full_name} AS\n"
                + view_sql.format(schema=schema)
            )
            try:
                conn.execute(text(ddl))
                print(f"[Gold]   View created/replaced: {full_name}")
            except Exception as exc:
                print(f"[Gold] Warning: could not create view {full_name}: {exc}")
    print("[Gold] Analysis views done.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_key_map(engine, schema: str, table: str, bk_col: str, sk_col: str) -> dict:
    """
    Return a dict mapping business-key values → surrogate-key values
    by querying the dimension table.
    """
    sql = f"SELECT {bk_col}, {sk_col} FROM {schema}.{table}"
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {str(bk): int(sk) for bk, sk in rows}


def _psycopg2_conn_str(engine) -> str:
    """Convert SQLAlchemy URL to a psycopg2-compatible DSN string."""
    url = engine.url.render_as_string(hide_password=False)
    # Strip the '+psycopg2' driver suffix that psycopg2 does not understand
    return url.replace("postgresql+psycopg2://", "postgresql://")


def _bulk_upsert(engine, sql: str, df: pd.DataFrame, batch_size: int = 500) -> None:
    """
    Execute a parameterised upsert statement in batches using psycopg2
    executemany.  Converts the DataFrame to a list of dicts first.
    """
    records = df.to_dict(orient="records")
    conn_str = _psycopg2_conn_str(engine)
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for i in range(0, len(records), batch_size):
                batch = records[i: i + batch_size]
                cur.executemany(sql, batch)
        conn.commit()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def load_to_gold(silver_dir: str, db_conn: str, schema: str) -> None:
    """
    End-to-end gold layer loader:
    1. Create schema and tables
    2. Populate dim_date
    3. Upsert dimension tables
    4. Upsert fact_sales
    5. Create/replace analysis views

    Parameters
    ----------
    silver_dir : path to the silver parquet directory
    db_conn    : SQLAlchemy connection string
    schema     : PostgreSQL schema for the data mart (e.g. 'retail_datamart')
    """
    print(f"[Gold] Starting gold load into schema '{schema}' …")
    engine = create_engine(db_conn)

    # 1. DDL
    create_schema_and_tables(engine, schema)

    # 2. Date dimension
    populate_dim_date(engine, schema)

    # 3. Dimensions
    print("\n[Gold] Loading dimensions …")
    upsert_dim_customer(engine, schema, silver_dir)
    upsert_dim_product(engine, schema, silver_dir)
    upsert_dim_store(engine, schema, silver_dir)

    # 4. Fact table
    print("\n[Gold] Loading fact table …")
    upsert_fact_sales(engine, schema, silver_dir)

    # 5. Views
    print("\n[Gold] Building analysis views …")
    create_analysis_views(engine, schema)

    engine.dispose()
    print("\n[Gold] Gold load complete.")
