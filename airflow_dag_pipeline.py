"""
airflow_dag_pipeline.py
------------------------
Apache Airflow DAG: Orchestrates the S3 → Snowflake ELT pipeline
Author: Hely Shah
Description:
    Defines a production-style Airflow DAG with:
    - Task dependencies
    - Automatic retries with exponential backoff
    - SLA alerts
    - Logging at every stage
    - Data quality gate before loading GOLD
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

# ─── Default Args (applies to all tasks) ─────────────────────────────────────
default_args = {
    "owner":              "hely.shah",
    "depends_on_past":    False,
    "email":              ["hely.shah.work@gmail.com"],
    "email_on_failure":   True,
    "email_on_retry":     False,
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),
    "retry_exponential_backoff": True,      # 5min → 10min → 20min
    "max_retry_delay":    timedelta(minutes=30),
    "execution_timeout":  timedelta(hours=2),
    "sla":                timedelta(hours=3),  # Alert if task exceeds 3 hours
}

# ─── SLA Miss Callback ────────────────────────────────────────────────────────
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.warning(f"SLA missed for tasks: {task_list}")
    send_email(
        to=["hely.shah.work@gmail.com"],
        subject=f"[AIRFLOW SLA MISS] {dag.dag_id}",
        html_content=f"<p>SLA missed for: {task_list}</p>"
    )

# ─── Task Functions ───────────────────────────────────────────────────────────
def task_extract(**context):
    """Extract raw CSVs from S3 and push row count to XCom."""
    logger.info("TASK: extract_from_s3 — starting")
    # In production: call extract_from_s3() from etl_pipeline_aws_snowflake.py
    # Simulated here for portability
    record_count = 15234  # replace with len(raw_df)
    logger.info(f"Extracted {record_count:,} records.")
    context["ti"].xcom_push(key="record_count", value=record_count)
    return "extraction_complete"


def task_load_bronze(**context):
    """Load raw data into Snowflake BRONZE layer."""
    logger.info("TASK: load_bronze — starting")
    record_count = context["ti"].xcom_pull(key="record_count", task_ids="extract")
    logger.info(f"Loading {record_count:,} records to BRONZE...")
    # In production: call load_bronze() from etl_pipeline_aws_snowflake.py
    logger.info("BRONZE load complete.")


def task_data_quality_check(**context):
    """
    Data quality gate between BRONZE and SILVER.
    Checks: record count threshold, null rate, duplicate rate.
    Returns branch name based on pass/fail.
    """
    logger.info("TASK: data_quality_check — running validation rules")
    record_count = context["ti"].xcom_pull(key="record_count", task_ids="extract")

    # Rule 1: Minimum record count
    if record_count < 100:
        logger.error(f"Quality gate FAILED: only {record_count} records (min 100 required)")
        return "quality_gate_failed"

    # Rule 2: Simulate null rate check (< 5% nulls allowed)
    null_rate = 0.02  # replace with actual: df[critical_cols].isnull().mean().max()
    if null_rate > 0.05:
        logger.error(f"Quality gate FAILED: null rate {null_rate:.1%} exceeds threshold")
        return "quality_gate_failed"

    # Rule 3: Simulate duplicate rate check (< 1% duplicates allowed)
    dup_rate = 0.003  # replace with actual: df.duplicated(subset=['ORDER_ID']).mean()
    if dup_rate > 0.01:
        logger.error(f"Quality gate FAILED: duplicate rate {dup_rate:.1%} exceeds threshold")
        return "quality_gate_failed"

    logger.info(f"Quality gate PASSED — null_rate={null_rate:.1%}, dup_rate={dup_rate:.1%}")
    context["ti"].xcom_push(key="quality_status", value="PASSED")
    return "load_silver"


def task_load_silver(**context):
    """Transform and load cleansed data into SILVER layer."""
    logger.info("TASK: load_silver — transforming and loading")
    # In production: call transform_to_silver() then load_silver()
    logger.info("SILVER load complete.")


def task_load_gold(**context):
    """Aggregate SILVER into analytics-ready GOLD layer."""
    logger.info("TASK: load_gold — aggregating revenue metrics")
    # In production: call transform_to_gold() then load_gold()
    logger.info("GOLD load complete.")


def task_quality_gate_failed(**context):
    """Handle quality gate failure — log, alert, stop pipeline."""
    logger.error("Pipeline halted at data quality gate. Manual review required.")
    raise ValueError("Data quality gate failed. Check logs for details.")


def task_notify_success(**context):
    """Send success notification."""
    logger.info("Pipeline completed successfully. Notifying stakeholders.")
    send_email(
        to=["hely.shah.work@gmail.com"],
        subject="[AIRFLOW SUCCESS] S3 → Snowflake ELT Pipeline",
        html_content="<p>ELT pipeline completed. BRONZE → SILVER → GOLD layers updated.</p>"
    )


# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="s3_snowflake_elt_pipeline",
    description="S3 → Snowflake ELT: Bronze / Silver / Gold with data quality gate",
    default_args=default_args,
    schedule_interval="0 6 * * *",      # Daily at 6:00 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "snowflake", "aws", "data-engineering"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ── Task 1: Extract from S3
    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        provide_context=True,
    )

    # ── Task 2: Load Bronze (raw)
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
        provide_context=True,
    )

    # ── Task 3: Data Quality Gate (branching)
    quality_check = BranchPythonOperator(
        task_id="data_quality_check",
        python_callable=task_data_quality_check,
        provide_context=True,
    )

    # ── Task 4a: Load Silver (quality passed)
    load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=task_load_silver,
        provide_context=True,
    )

    # ── Task 4b: Quality Gate Failed
    quality_gate_failed = PythonOperator(
        task_id="quality_gate_failed",
        python_callable=task_quality_gate_failed,
        provide_context=True,
    )

    # ── Task 5: Load Gold (aggregated)
    load_gold = PythonOperator(
        task_id="load_gold",
        python_callable=task_load_gold,
        provide_context=True,
    )

    # ── Task 6: Notify Success
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify_success,
        provide_context=True,
    )

    # ─── DAG Dependencies ──────────────────────────────────────────────────────
    # extract → load_bronze → quality_check → [load_silver | quality_gate_failed]
    # load_silver → load_gold → notify_success
    extract >> load_bronze >> quality_check
    quality_check >> load_silver >> load_gold >> notify_success
    quality_check >> quality_gate_failed
