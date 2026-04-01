"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


# ─────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────
def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames."""
    tables = ["customers", "products", "orders", "order_items"]

    data = {}
    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"Extracted {table}: {len(df)} rows")
        data[table] = df

    return data


# ─────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────
def transform(data_dict):
    """Transform raw data into customer-level analytics summary."""

    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # 1. Filter bad data
    orders = orders[orders["status"] != "cancelled"]
    order_items = order_items[order_items["quantity"] <= 100]

    # 2. Join tables
    df = orders.merge(order_items, on="order_id")
    df = df.merge(products, on="product_id")
    df = df.merge(customers, on="customer_id")

    print(f"After joins: {len(df)} rows")

    # 3. Compute line_total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # 4. Aggregate to customer level
    summary = df.groupby(
        ["customer_id", "customer_name", "city"], dropna=False
    ).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    ).reset_index()

    # 5. Average order value
    summary["avg_order_value"] = (
        summary["total_revenue"] / summary["total_orders"]
    )

    # 6. Top category per customer
    category_rev = df.groupby(
        ["customer_id", "category"]
    )["line_total"].sum().reset_index()

    top_category = category_rev.sort_values(
        ["customer_id", "line_total"],
        ascending=[True, False]
    ).drop_duplicates("customer_id")

    top_category = top_category[["customer_id", "category"]].rename(
        columns={"category": "top_category"}
    )

    summary = summary.merge(top_category, on="customer_id")

    print(f"Transformed: {len(summary)} customers")

    return summary


# ─────────────────────────────────────────────────────────
# VALIDATE
# ─────────────────────────────────────────────────────────
def validate(df):
    """Run data quality checks on the transformed DataFrame."""

    checks = {}

    checks["no_null_customer_id"] = df["customer_id"].notnull().all()
    checks["no_null_customer_name"] = df["customer_name"].notnull().all()
    checks["positive_revenue"] = (df["total_revenue"] > 0).all()
    checks["no_duplicate_customer_id"] = df["customer_id"].is_unique
    checks["positive_orders"] = (df["total_orders"] > 0).all()

    print("\nValidation Results:")
    for k, v in checks.items():
        print(f"{k}: {'PASS' if v else 'FAIL'}")

    if not all(checks.values()):
        raise ValueError("Validation failed")

    return checks


# ─────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────
def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file."""

    # Ensure output directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Write to database
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)

    # Write to CSV
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows to DB and CSV")


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    """Orchestrate the ETL pipeline."""

    print("Starting ETL pipeline...")

    # Create engine
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    )
    engine = create_engine(DATABASE_URL)

    # Run ETL steps
    data = extract(engine)
    df = transform(data)
    validate(df)
    load(df, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()