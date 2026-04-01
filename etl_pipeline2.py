import pandas as pd
import numpy as np
import os
import json
import datetime
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

DB_URI = "postgresql+psycopg2://postgres:postgres@localhost:5432/amman_market"
engine = create_engine(DB_URI)

# ─────────────────────────────────────────────
# LOAD CONFIG (TIER 3)
# ─────────────────────────────────────────────

def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

# ─────────────────────────────────────────────
# METADATA (TIER 2)
# ─────────────────────────────────────────────

def get_last_run(engine):
    query = "SELECT MAX(end_time) FROM etl_metadata"
    result = pd.read_sql(query, engine).iloc[0, 0]
    return result

# ─────────────────────────────────────────────
# FILTER ENGINE (TIER 3)
# ─────────────────────────────────────────────

def apply_filters(df, filters):
    for f in filters:
        col = f["column"]
        op = f["operator"]
        val = f["value"]

        if op == "==":
            df = df[df[col] == val]
        elif op == "!=":
            df = df[df[col] != val]
        elif op == ">":
            df = df[df[col] > val]
        elif op == "<":
            df = df[df[col] < val]
        elif op == ">=":
            df = df[df[col] >= val]
        elif op == "<=":
            df = df[df[col] <= val]

    return df

# ─────────────────────────────────────────────
# AGGREGATION ENGINE (TIER 3)
# ─────────────────────────────────────────────

def apply_aggregation(df, config):

    groupby_cols = config["groupby"]
    aggregations = config["aggregations"]

    agg_dict = {}
    for new_col, (col, func) in aggregations.items():
        agg_dict[new_col] = (col, func)

    return df.groupby(groupby_cols).agg(**agg_dict).reset_index()

# ─────────────────────────────────────────────
# EXTRACT (TIER 2)
# ─────────────────────────────────────────────

def extract():
    last_run = get_last_run(engine)

    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)

    if last_run:
        print(f"Incremental run since: {last_run}")

        orders = pd.read_sql(
            text("SELECT * FROM orders WHERE order_date > :last_run"),
            engine,
            params={"last_run": last_run}
        )
    else:
        print("Full load (first run)")
        orders = pd.read_sql("SELECT * FROM orders", engine)

    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    print(f"Extracted customers: {len(customers)} rows")
    print(f"Extracted products: {len(products)} rows")
    print(f"Extracted orders: {len(orders)} rows")
    print(f"Extracted order_items: {len(order_items)} rows")

    return customers, products, orders, order_items

# ─────────────────────────────────────────────
# TRANSFORM (TIER 3)
# ─────────────────────────────────────────────

def transform(customers, products, orders, order_items):

    df = orders.merge(customers, on="customer_id", how="inner")
    df = df.merge(order_items, on="order_id", how="inner")
    df = df.merge(products, on="product_id", how="inner")

    print(f"After joins: {len(df)} rows")

    # Add revenue
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Load config
    config = load_config()

    # Apply filters
    df = apply_filters(df, config["filters"])
    print(f"After filtering: {len(df)} rows")

    # Apply aggregation
    summary = apply_aggregation(df, config)

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    print(f"Transformed: {len(summary)} customers")

    # Outlier detection (Tier 1)
    mean = summary["total_revenue"].mean()
    std = summary["total_revenue"].std()

    summary["is_outlier"] = summary["total_revenue"] > (mean + 3 * std)

    return summary


# ─────────────────────────────────────────────
# VALIDATION (TIER 1)
# ─────────────────────────────────────────────

def validate(df):

    results = {
        "no_null_customer_id": bool(not df["customer_id"].isnull().any()),
        "no_null_customer_name": bool(not df["customer_name"].isnull().any()),
        "positive_revenue": bool((df["total_revenue"] > 0).all()),
        "no_duplicate_customer_id": bool(df["customer_id"].is_unique),
        "positive_orders": bool((df["total_orders"] > 0).all())
    }

    print("\nValidation Results:")
    for k, v in results.items():
        print(f"{k}: {'PASS' if v else 'FAIL'}")

    return results

# ─────────────────────────────────────────────
# QUALITY REPORT (TIER 1)
# ─────────────────────────────────────────────

def generate_quality_report(df, validation_results):

    os.makedirs("output", exist_ok=True)

    report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "total_rows": int(len(df)),
        "validation_results": validation_results,
        "outliers": df[df["is_outlier"]][["customer_id", "total_revenue"]].to_dict(orient="records")
    }

    with open("output/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    print("Quality report generated at output/quality_report.json")

# ─────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────

def load(df):

    os.makedirs("output", exist_ok=True)

    df.to_csv("output/customer_analytics.csv", index=False)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)

    print(f"Loaded {len(df)} rows to DB and CSV")

# ─────────────────────────────────────────────
# MAIN (TIER 2 + TIER 3)
# ─────────────────────────────────────────────

def main():

    import datetime

    start_time = datetime.datetime.now()
    transformed_df = None
    status = "success"

    try:
        customers, products, orders, order_items = extract()

        transformed_df = transform(customers, products, orders, order_items)

        validation_results = validate(transformed_df)

        generate_quality_report(transformed_df, validation_results)

        load(transformed_df)

    except Exception as e:
        status = "failed"
        raise e

    finally:
        end_time = datetime.datetime.now()

        rows = len(transformed_df) if transformed_df is not None else 0

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
                    VALUES (:start_time, :end_time, :rows, :status)
                """),
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "rows": rows,
                    "status": status
                }
            )

    print("ETL pipeline completed successfully!")

# ─────────────────────────────────────────────

if __name__ == "__main__":
    main()