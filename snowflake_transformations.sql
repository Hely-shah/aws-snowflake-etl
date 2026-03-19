-- ============================================================
-- snowflake_transformations.sql
-- Author: Hely Shah
-- Description:
--   ELT transformations across Snowflake Bronze → Silver → Gold layers.
--   Implements Lakehouse pattern with:
--   - Raw ingestion (BRONZE)
--   - Cleansed & validated datasets (SILVER)
--   - Analytics-ready aggregations (GOLD)
--   - Data quality metadata columns throughout
-- ============================================================


-- ============================================================
-- LAYER 1: BRONZE — Raw ingestion (no transformations)
-- ============================================================

CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.GOLD;

-- Raw orders table (loaded by Python ETL from S3)
CREATE OR REPLACE TABLE ANALYTICS_DB.BRONZE.RAW_ORDERS (
    ORDER_ID        STRING,
    CUSTOMER_ID     STRING,
    ORDER_DATE      STRING,         -- Raw string; cast in SILVER
    AMOUNT          STRING,         -- Raw string; cast in SILVER
    QUANTITY        STRING,
    UNIT_PRICE      STRING,
    CUSTOMER_SEGMENT STRING,
    STATUS          STRING,
    _SOURCE_FILE    STRING,
    _INGESTED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify Bronze row count
SELECT
    COUNT(*)                                    AS total_rows,
    COUNT(DISTINCT ORDER_ID)                    AS unique_orders,
    SUM(CASE WHEN ORDER_ID IS NULL THEN 1 END)  AS null_order_ids,
    MIN(_INGESTED_AT)                           AS first_ingested,
    MAX(_INGESTED_AT)                           AS last_ingested
FROM ANALYTICS_DB.BRONZE.RAW_ORDERS;


-- ============================================================
-- LAYER 2: SILVER — Cleansed, typed, validated
-- ============================================================

CREATE OR REPLACE TABLE ANALYTICS_DB.SILVER.CLEAN_ORDERS AS
WITH

-- Step 1: Cast types and flag nulls
typed AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')           AS ORDER_DATE,
        TRY_TO_NUMBER(AMOUNT, 10, 2)                    AS AMOUNT,
        TRY_TO_NUMBER(QUANTITY)                         AS QUANTITY,
        TRY_TO_NUMBER(UNIT_PRICE, 10, 2)                AS UNIT_PRICE,
        UPPER(TRIM(CUSTOMER_SEGMENT))                   AS CUSTOMER_SEGMENT,
        UPPER(TRIM(STATUS))                             AS STATUS,
        _SOURCE_FILE,
        _INGESTED_AT,
        -- Null flags for observability
        (ORDER_ID IS NULL)                              AS IS_NULL_ORDER_ID,
        (ORDER_DATE IS NULL OR TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD') IS NULL) AS IS_NULL_ORDER_DATE,
        (TRY_TO_NUMBER(AMOUNT, 10, 2) IS NULL)          AS IS_NULL_AMOUNT
    FROM ANALYTICS_DB.BRONZE.RAW_ORDERS
),

-- Step 2: Deduplicate on ORDER_ID (keep latest ingestion)
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID
            ORDER BY _INGESTED_AT DESC
        ) AS _row_num
    FROM typed
),

-- Step 3: Apply quality rules — remove nulls and invalid rows
validated AS (
    SELECT *
    FROM deduped
    WHERE _row_num = 1
      AND ORDER_ID IS NOT NULL
      AND ORDER_DATE IS NOT NULL
      AND AMOUNT IS NOT NULL
      AND AMOUNT >= 0              -- No negative amounts
      AND AMOUNT <= 100000         -- Upper bound validation
)

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    DATE_TRUNC('MONTH', ORDER_DATE)                 AS ORDER_MONTH,
    YEAR(ORDER_DATE)                                AS ORDER_YEAR,
    AMOUNT,
    QUANTITY,
    UNIT_PRICE,
    -- KPI validation: flag where REVENUE ≠ QUANTITY × UNIT_PRICE
    ABS(AMOUNT - (QUANTITY * UNIT_PRICE)) > 0.01    AS KPI_MISMATCH_FLAG,
    CUSTOMER_SEGMENT,
    STATUS,
    _SOURCE_FILE,
    _INGESTED_AT,
    CURRENT_TIMESTAMP()                             AS SILVER_LOADED_AT,
    'SILVER'                                        AS LAYER
FROM validated;

-- Silver data quality summary
SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(DISTINCT ORDER_ID)                        AS unique_orders,
    SUM(CASE WHEN KPI_MISMATCH_FLAG THEN 1 END)     AS kpi_mismatches,
    AVG(AMOUNT)                                     AS avg_order_value,
    MIN(ORDER_DATE)                                 AS earliest_order,
    MAX(ORDER_DATE)                                 AS latest_order
FROM ANALYTICS_DB.SILVER.CLEAN_ORDERS;


-- ============================================================
-- LAYER 3: GOLD — Analytics-ready aggregations
-- ============================================================

-- Gold Table 1: Monthly Revenue by Customer Segment
CREATE OR REPLACE TABLE ANALYTICS_DB.GOLD.MONTHLY_REVENUE_BY_SEGMENT AS
SELECT
    ORDER_MONTH,
    ORDER_YEAR,
    CUSTOMER_SEGMENT,
    COUNT(DISTINCT ORDER_ID)                        AS ORDER_COUNT,
    SUM(AMOUNT)                                     AS TOTAL_REVENUE,
    ROUND(AVG(AMOUNT), 2)                           AS AVG_ORDER_VALUE,
    ROUND(MEDIAN(AMOUNT), 2)                        AS MEDIAN_ORDER_VALUE,
    SUM(QUANTITY)                                   AS TOTAL_UNITS_SOLD,
    -- Month-over-month revenue (for BI dashboards)
    LAG(SUM(AMOUNT)) OVER (
        PARTITION BY CUSTOMER_SEGMENT
        ORDER BY ORDER_MONTH
    )                                               AS PREV_MONTH_REVENUE,
    ROUND(
        (SUM(AMOUNT) - LAG(SUM(AMOUNT)) OVER (
            PARTITION BY CUSTOMER_SEGMENT ORDER BY ORDER_MONTH
        )) / NULLIF(LAG(SUM(AMOUNT)) OVER (
            PARTITION BY CUSTOMER_SEGMENT ORDER BY ORDER_MONTH
        ), 0) * 100, 2
    )                                               AS MOM_REVENUE_GROWTH_PCT,
    CURRENT_TIMESTAMP()                             AS GOLD_LOADED_AT,
    'GOLD'                                          AS LAYER
FROM ANALYTICS_DB.SILVER.CLEAN_ORDERS
WHERE STATUS != 'CANCELLED'
GROUP BY ORDER_MONTH, ORDER_YEAR, CUSTOMER_SEGMENT
ORDER BY ORDER_MONTH, CUSTOMER_SEGMENT;


-- Gold Table 2: Customer-Level KPIs (for Power BI dashboard)
CREATE OR REPLACE TABLE ANALYTICS_DB.GOLD.CUSTOMER_KPI_SUMMARY AS
SELECT
    CUSTOMER_ID,
    CUSTOMER_SEGMENT,
    COUNT(DISTINCT ORDER_ID)                        AS TOTAL_ORDERS,
    SUM(AMOUNT)                                     AS LIFETIME_VALUE,
    ROUND(AVG(AMOUNT), 2)                           AS AVG_ORDER_VALUE,
    MIN(ORDER_DATE)                                 AS FIRST_ORDER_DATE,
    MAX(ORDER_DATE)                                 AS LAST_ORDER_DATE,
    DATEDIFF('day', MIN(ORDER_DATE), MAX(ORDER_DATE)) AS CUSTOMER_TENURE_DAYS,
    -- Activity classification
    CASE
        WHEN MAX(ORDER_DATE) >= DATEADD('day', -30, CURRENT_DATE())  THEN 'ACTIVE'
        WHEN MAX(ORDER_DATE) >= DATEADD('day', -90, CURRENT_DATE())  THEN 'AT_RISK'
        ELSE 'CHURNED'
    END                                             AS CUSTOMER_STATUS,
    CURRENT_TIMESTAMP()                             AS GOLD_LOADED_AT
FROM ANALYTICS_DB.SILVER.CLEAN_ORDERS
GROUP BY CUSTOMER_ID, CUSTOMER_SEGMENT
ORDER BY LIFETIME_VALUE DESC;


-- ============================================================
-- OBSERVABILITY — Pipeline Audit Log
-- ============================================================

CREATE OR REPLACE TABLE ANALYTICS_DB.GOLD.PIPELINE_AUDIT_LOG (
    PIPELINE_RUN_ID     STRING DEFAULT UUID_STRING(),
    LAYER               STRING,
    TABLE_NAME          STRING,
    ROW_COUNT           NUMBER,
    NULL_RATE           FLOAT,
    DUPLICATE_RATE      FLOAT,
    KPI_MISMATCH_COUNT  NUMBER,
    STATUS              STRING,
    RUN_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    NOTES               STRING
);

-- Insert audit entry after each layer load (call from Python ETL)
INSERT INTO ANALYTICS_DB.GOLD.PIPELINE_AUDIT_LOG
    (LAYER, TABLE_NAME, ROW_COUNT, STATUS, NOTES)
SELECT
    'SILVER',
    'CLEAN_ORDERS',
    COUNT(*),
    'LOADED',
    'Bronze → Silver transformation complete'
FROM ANALYTICS_DB.SILVER.CLEAN_ORDERS;


-- ============================================================
-- RECONCILIATION — Cross-layer count comparison
-- ============================================================

SELECT
    'BRONZE'    AS LAYER,
    COUNT(*)    AS ROW_COUNT
FROM ANALYTICS_DB.BRONZE.RAW_ORDERS
UNION ALL
SELECT
    'SILVER',
    COUNT(*)
FROM ANALYTICS_DB.SILVER.CLEAN_ORDERS
UNION ALL
SELECT
    'GOLD (revenue)',
    COUNT(*)
FROM ANALYTICS_DB.GOLD.MONTHLY_REVENUE_BY_SEGMENT
ORDER BY LAYER;
