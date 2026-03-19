"""
etl_pipeline_aws_snowflake.py
--------------------------------
ELT Pipeline: AWS S3 → Snowflake (Bronze → Silver → Gold)
Author: Hely Shah
Description:
    Ingests raw CSV data from S3, validates and transforms using Pandas,
    then loads into Snowflake Bronze, Silver, and Gold layers.
    Demonstrates: boto3, Snowflake connector, ELT patterns, data quality checks.
"""

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from io import StringIO
import logging
import os
from datetime import datetime

# ─── Logging Setup ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ─── Config (use environment variables in production) ─────────────────────────
AWS_CONFIG = {
    "bucket_name":       os.getenv("S3_BUCKET", "your-data-bucket"),
    "raw_prefix":        "raw/orders/",
    "region_name":       os.getenv("AWS_REGION", "us-east-1"),
}

SNOWFLAKE_CONFIG = {
    "user":        os.getenv("SNOWFLAKE_USER", "your_user"),
    "password":    os.getenv("SNOWFLAKE_PASSWORD", "your_password"),
    "account":     os.getenv("SNOWFLAKE_ACCOUNT", "your_account"),
    "warehouse":   "COMPUTE_WH",
    "database":    "ANALYTICS_DB",
    "schema":      "BRONZE",
    "role":        "DATA_ENGINEER",
}

# ─── Step 1: Extract — Read CSV from S3 ──────────────────────────────────────
def extract_from_s3(bucket: str, prefix: str) -> pd.DataFrame:
    """Read all CSV files from an S3 prefix into a single DataFrame."""
    logger.info(f"Connecting to S3 bucket: {bucket}, prefix: {prefix}")
    s3_client = boto3.client("s3", region_name=AWS_CONFIG["region_name"])

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

    if not files:
        raise FileNotFoundError(f"No CSV files found in s3://{bucket}/{prefix}")

    frames = []
    for key in files:
        logger.info(f"Reading s3://{bucket}/{key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
        df["_source_file"] = key
        df["_ingested_at"] = datetime.utcnow()
        frames.append(df)

    raw_df = pd.concat(frames, ignore_index=True)
    logger.info(f"Extracted {len(raw_df):,} records from {len(files)} file(s).")
    return raw_df


# ─── Step 2: Bronze Load — Raw data as-is ────────────────────────────────────
def load_bronze(df: pd.DataFrame, conn) -> None:
    """Load raw data into Snowflake BRONZE layer with no transformations."""
    logger.info("Loading to BRONZE layer...")
    df.columns = [c.upper().replace(" ", "_") for c in df.columns]
    df["LAYER"] = "BRONZE"

    conn.cursor().execute("USE SCHEMA ANALYTICS_DB.BRONZE")
    success, nchunks, nrows, _ = write_pandas(conn, df, "RAW_ORDERS", auto_create_table=True)
    logger.info(f"BRONZE: {nrows:,} rows loaded in {nchunks} chunk(s). Success={success}")


# ─── Step 3: Transform — Data Quality + Cleansing ────────────────────────────
def transform_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data quality rules and cleansing for SILVER layer.
    Rules:
      - Drop duplicates on ORDER_ID
      - Nulls in critical columns are flagged and removed
      - Standardize column types
      - Add reconciliation metadata
    """
    logger.info("Transforming to SILVER layer...")
    initial_count = len(df)

    # Rule 1: Drop duplicates
    df = df.drop_duplicates(subset=["ORDER_ID"])
    logger.info(f"  Deduplication: {initial_count - len(df)} rows removed.")

    # Rule 2: Drop nulls in critical columns
    critical_cols = ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "AMOUNT"]
    before = len(df)
    df = df.dropna(subset=[c for c in critical_cols if c in df.columns])
    logger.info(f"  Null removal: {before - len(df)} rows removed.")

    # Rule 3: Type casting
    if "ORDER_DATE" in df.columns:
        df["ORDER_DATE"] = pd.to_datetime(df["ORDER_DATE"], errors="coerce")
    if "AMOUNT" in df.columns:
        df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce")

    # Rule 4: Reconciliation metadata
    df["RECORD_COUNT"]     = len(df)
    df["VALIDATION_STATUS"] = "PASSED"
    df["SILVER_LOADED_AT"]  = datetime.utcnow()
    df["LAYER"]             = "SILVER"

    logger.info(f"SILVER transformation complete. {len(df):,} records ready.")
    return df


# ─── Step 4: Silver Load ──────────────────────────────────────────────────────
def load_silver(df: pd.DataFrame, conn) -> None:
    logger.info("Loading to SILVER layer...")
    conn.cursor().execute("USE SCHEMA ANALYTICS_DB.SILVER")
    success, nchunks, nrows, _ = write_pandas(conn, df, "CLEAN_ORDERS", auto_create_table=True)
    logger.info(f"SILVER: {nrows:,} rows loaded. Success={success}")


# ─── Step 5: Gold Aggregation — Analytics-Ready ──────────────────────────────
def transform_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate SILVER data into analytics-ready GOLD layer.
    Produces: daily revenue by customer segment.
    """
    logger.info("Aggregating to GOLD layer...")

    if "ORDER_DATE" in df.columns and "AMOUNT" in df.columns:
        df["ORDER_DATE"] = pd.to_datetime(df["ORDER_DATE"])
        df["ORDER_MONTH"] = df["ORDER_DATE"].dt.to_period("M").astype(str)

        gold_df = df.groupby(["ORDER_MONTH", "CUSTOMER_SEGMENT"] if "CUSTOMER_SEGMENT" in df.columns else ["ORDER_MONTH"]).agg(
            TOTAL_REVENUE=("AMOUNT", "sum"),
            ORDER_COUNT=("ORDER_ID", "count"),
            AVG_ORDER_VALUE=("AMOUNT", "mean"),
        ).reset_index()

        gold_df["GOLD_LOADED_AT"] = datetime.utcnow()
        gold_df["LAYER"] = "GOLD"
        logger.info(f"GOLD aggregation complete. {len(gold_df):,} summary rows.")
        return gold_df

    logger.warning("ORDER_DATE or AMOUNT missing — skipping GOLD aggregation.")
    return pd.DataFrame()


# ─── Step 6: Gold Load ────────────────────────────────────────────────────────
def load_gold(df: pd.DataFrame, conn) -> None:
    if df.empty:
        logger.warning("GOLD DataFrame is empty — skipping load.")
        return
    logger.info("Loading to GOLD layer...")
    conn.cursor().execute("USE SCHEMA ANALYTICS_DB.GOLD")
    success, nchunks, nrows, _ = write_pandas(conn, df, "MONTHLY_REVENUE_SUMMARY", auto_create_table=True)
    logger.info(f"GOLD: {nrows:,} rows loaded. Success={success}")


# ─── Orchestrator ────────────────────────────────────────────────────────────
def run_pipeline():
    logger.info("=" * 60)
    logger.info("Starting ELT Pipeline: S3 → Snowflake Bronze/Silver/Gold")
    logger.info("=" * 60)

    # Extract
    raw_df = extract_from_s3(
        bucket=AWS_CONFIG["bucket_name"],
        prefix=AWS_CONFIG["raw_prefix"]
    )

    # Connect to Snowflake
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    try:
        # Bronze: raw load
        load_bronze(raw_df.copy(), conn)

        # Silver: cleaned & validated
        silver_df = transform_to_silver(raw_df.copy())
        load_silver(silver_df, conn)

        # Gold: aggregated
        gold_df = transform_to_gold(silver_df.copy())
        load_gold(gold_df, conn)

        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

    finally:
        conn.close()
        logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    run_pipeline()
