# AWS–Snowflake ELT Pipeline

**Author:** Hely Shah  
**Stack:** Python · AWS S3 · Apache Airflow · Snowflake · SQL · Data Quality

---

## Overview

End-to-end ELT pipeline that ingests raw CSV data from **AWS S3**, applies data quality validation, and loads analytics-ready datasets into **Snowflake** across Bronze → Silver → Gold layers (Lakehouse pattern).

Orchestrated by **Apache Airflow** with automatic retries, SLA alerts, and a data quality gate between Bronze and Silver.

---

## Architecture

```
S3 (raw CSVs)
     │
     ▼
[Extract — boto3]
     │
     ▼
SNOWFLAKE BRONZE  ◄─── Raw, unmodified data
     │
     ▼
[Data Quality Gate — Airflow BranchOperator]
     │         │
   PASS       FAIL ──► Alert + halt
     │
     ▼
SNOWFLAKE SILVER  ◄─── Typed, deduplicated, validated
     │
     ▼
SNOWFLAKE GOLD    ◄─── Aggregated KPIs, revenue summaries
     │
     ▼
Power BI Dashboard
```

---

## Files

| File | Description |
|------|-------------|
| `etl_pipeline_aws_snowflake.py` | Core ELT: S3 extract → Bronze/Silver/Gold load |
| `airflow_dag_pipeline.py` | Airflow DAG: orchestration, retries, branching |
| `data_quality_checks.py` | Data quality engine: null rate, duplicates, reconciliation, KPI logic |
| `snowflake_transformations.sql` | SQL: Bronze → Silver → Gold transformations + audit log |

---

## Data Quality Checks

| Check | Rule | Threshold |
|-------|------|-----------|
| Null rate | Critical columns must not exceed null % | 5% |
| Duplicates | Dedup on ORDER_ID | 1% |
| Cross-source reconciliation | Count match across SolarWinds / AMS / Amdocs | 2% |
| Value range | AMOUNT between 0 and 100,000 | — |
| Schema drift | Detect missing or unexpected columns | — |
| KPI logic | REVENUE = QUANTITY × UNIT_PRICE | ±$0.01 |

---

## How to Run

### 1. Set environment variables
```bash
export S3_BUCKET=your-data-bucket
export AWS_REGION=us-east-1
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=your_account
```

### 2. Install dependencies
```bash
pip install boto3 pandas snowflake-connector-python apache-airflow
```

### 3. Run ETL pipeline directly
```bash
python etl_pipeline_aws_snowflake.py
```

### 4. Run data quality checks
```bash
python data_quality_checks.py
```

### 5. Deploy Airflow DAG
```bash
cp airflow_dag_pipeline.py $AIRFLOW_HOME/dags/
airflow dags trigger s3_snowflake_elt_pipeline
```

### 6. Run Snowflake SQL
Open `snowflake_transformations.sql` in Snowflake worksheet and execute layer by layer.

---
---

## Project Methodology

**Requirement Gathering**
- Defined pipeline business requirements before implementation: data sources, transformation rules, quality thresholds, and Gold layer output schema
- Documented Bronze/Silver/Gold layer specifications and KPI logic rules as formal requirements
- Identified data quality requirements: null rate thresholds (5%), deduplication rules, cross-source reconciliation logic

**Agile / DevOps Workflow**
- Followed iterative development cycles: built and validated Bronze ingestion first, then Silver transformation, then Gold aggregation
- Implemented CI/CD pipeline using GitHub Actions — every commit triggers automated validation
- Used version-controlled, environment-variable-driven config to support reproducible builds across environments

**Business Requirements Addressed**
- Zero invalid records reaching the Gold analytics layer
- Automated data quality enforcement replacing manual validation checks
- Infrastructure fully reproducible via single Terraform command for any environment

---

## Skills Demonstrated
- **ELT / Lakehouse patterns** — Bronze → Silver → Gold architecture in Snowflake
- **AWS** — S3 ingestion with boto3, Airflow MWAA-compatible DAG
- **Data quality & observability** — multi-rule validation engine, reconciliation, KPI logic checks
- **Orchestration** — Airflow DAG with retries, SLA, branching on quality gate
- **CI/CD ready** — Git-based workflow, environment-variable config, no hardcoded credentials
- **SQL performance** — CTEs, window functions (LAG, ROW_NUMBER), deduplication, type casting
