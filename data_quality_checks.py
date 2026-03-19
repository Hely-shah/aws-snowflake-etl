"""
data_quality_checks.py
------------------------
Data Quality & Reconciliation Framework
Author: Hely Shah
Description:
    Multi-rule validation engine for Snowflake Bronze–Silver–Gold pipelines.
    Mirrors real-world reconciliation work done at Hotwire Communications:
    resolving multi-source mismatches across SolarWinds, AMS, Amdocs, ServiceNow.
    
    Checks:
      - Null rate per column
      - Duplicate key detection  
      - Cross-source reconciliation (record count match)
      - Value range / threshold validation
      - Schema drift detection
      - KPI logic validation
"""

import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ─── Result Model ─────────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    check_name:   str
    status:       str          # PASSED | FAILED | WARNING
    message:      str
    rows_affected: int = 0
    details:      Dict[str, Any] = field(default_factory=dict)
    timestamp:    str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ─── Data Quality Engine ──────────────────────────────────────────────────────
class DataQualityEngine:
    """
    Runs a suite of validation checks on a DataFrame and
    returns a full reconciliation report.
    """

    def __init__(self, df: pd.DataFrame, source_name: str = "unknown"):
        self.df = df.copy()
        self.source_name = source_name
        self.results: List[CheckResult] = []
        logger.info(f"DataQualityEngine initialized for source: {source_name} ({len(df):,} rows)")

    # ── Check 1: Null Rate ──────────────────────────────────────────────────
    def check_null_rate(self, columns: List[str], threshold: float = 0.05) -> CheckResult:
        """Flag columns where null rate exceeds threshold."""
        flagged = {}
        for col in columns:
            if col not in self.df.columns:
                continue
            rate = self.df[col].isnull().mean()
            if rate > threshold:
                flagged[col] = f"{rate:.1%}"

        if flagged:
            result = CheckResult(
                check_name="null_rate_check",
                status="FAILED",
                message=f"Null rate exceeded {threshold:.0%} threshold in: {list(flagged.keys())}",
                rows_affected=int(self.df[list(flagged.keys())].isnull().any(axis=1).sum()),
                details={"flagged_columns": flagged, "threshold": threshold}
            )
        else:
            result = CheckResult(
                check_name="null_rate_check",
                status="PASSED",
                message=f"All columns within {threshold:.0%} null threshold."
            )
        self.results.append(result)
        logger.info(f"[{result.status}] null_rate_check — {result.message}")
        return result

    # ── Check 2: Duplicate Keys ─────────────────────────────────────────────
    def check_duplicates(self, key_columns: List[str], threshold: float = 0.01) -> CheckResult:
        """Detect duplicate records on key columns."""
        valid_keys = [c for c in key_columns if c in self.df.columns]
        if not valid_keys:
            result = CheckResult("duplicate_check", "WARNING", "Key columns not found in DataFrame.")
            self.results.append(result)
            return result

        dup_mask = self.df.duplicated(subset=valid_keys, keep=False)
        dup_count = dup_mask.sum()
        dup_rate = dup_count / len(self.df)

        if dup_rate > threshold:
            result = CheckResult(
                check_name="duplicate_check",
                status="FAILED",
                message=f"Duplicate rate {dup_rate:.2%} exceeds {threshold:.0%} on keys: {valid_keys}",
                rows_affected=dup_count,
                details={"duplicate_rate": round(dup_rate, 4), "threshold": threshold}
            )
        else:
            result = CheckResult(
                check_name="duplicate_check",
                status="PASSED",
                message=f"Duplicate rate {dup_rate:.2%} within threshold.",
                rows_affected=dup_count
            )
        self.results.append(result)
        logger.info(f"[{result.status}] duplicate_check — {result.message}")
        return result

    # ── Check 3: Cross-Source Reconciliation ────────────────────────────────
    def check_record_reconciliation(
        self, source_a_count: int, source_b_count: int,
        source_a_name: str = "Source A", source_b_name: str = "Source B",
        tolerance_pct: float = 0.02
    ) -> CheckResult:
        """
        Reconcile record counts between two sources.
        Used to validate counts match across SolarWinds, AMS, Amdocs, ServiceNow.
        """
        diff = abs(source_a_count - source_b_count)
        diff_pct = diff / max(source_a_count, 1)

        if diff_pct > tolerance_pct:
            result = CheckResult(
                check_name="cross_source_reconciliation",
                status="FAILED",
                message=(f"{source_a_name} ({source_a_count:,}) vs {source_b_name} ({source_b_count:,}) "
                         f"differ by {diff:,} rows ({diff_pct:.1%}) — exceeds {tolerance_pct:.0%} tolerance"),
                rows_affected=diff,
                details={"source_a": source_a_count, "source_b": source_b_count,
                         "diff": diff, "diff_pct": round(diff_pct, 4)}
            )
        else:
            result = CheckResult(
                check_name="cross_source_reconciliation",
                status="PASSED",
                message=f"Counts reconciled within {tolerance_pct:.0%} tolerance. Diff: {diff:,} rows ({diff_pct:.1%}).",
                details={"source_a": source_a_count, "source_b": source_b_count}
            )
        self.results.append(result)
        logger.info(f"[{result.status}] cross_source_reconciliation — {result.message}")
        return result

    # ── Check 4: Value Range Validation ─────────────────────────────────────
    def check_value_ranges(self, column: str, min_val: float = None, max_val: float = None) -> CheckResult:
        """Validate numeric column stays within expected range."""
        if column not in self.df.columns:
            result = CheckResult("value_range_check", "WARNING", f"Column '{column}' not found.")
            self.results.append(result)
            return result

        series = pd.to_numeric(self.df[column], errors="coerce")
        out_of_range = pd.Series([False] * len(series))
        if min_val is not None:
            out_of_range |= series < min_val
        if max_val is not None:
            out_of_range |= series > max_val

        count = out_of_range.sum()
        if count > 0:
            result = CheckResult(
                check_name="value_range_check",
                status="FAILED",
                message=f"{count:,} rows in '{column}' outside range [{min_val}, {max_val}].",
                rows_affected=count,
                details={"column": column, "min": min_val, "max": max_val,
                         "actual_min": float(series.min()), "actual_max": float(series.max())}
            )
        else:
            result = CheckResult(
                check_name="value_range_check",
                status="PASSED",
                message=f"All values in '{column}' within range [{min_val}, {max_val}]."
            )
        self.results.append(result)
        logger.info(f"[{result.status}] value_range_check[{column}] — {result.message}")
        return result

    # ── Check 5: Schema Drift Detection ─────────────────────────────────────
    def check_schema_drift(self, expected_columns: List[str]) -> CheckResult:
        """Detect missing or unexpected columns vs expected schema."""
        actual_cols = set(self.df.columns)
        expected_cols = set(expected_columns)
        missing = expected_cols - actual_cols
        unexpected = actual_cols - expected_cols

        if missing:
            result = CheckResult(
                check_name="schema_drift_check",
                status="FAILED",
                message=f"Schema drift detected. Missing columns: {sorted(missing)}",
                details={"missing": sorted(missing), "unexpected": sorted(unexpected)}
            )
        elif unexpected:
            result = CheckResult(
                check_name="schema_drift_check",
                status="WARNING",
                message=f"Unexpected columns found (may be new fields): {sorted(unexpected)}",
                details={"missing": [], "unexpected": sorted(unexpected)}
            )
        else:
            result = CheckResult(
                check_name="schema_drift_check",
                status="PASSED",
                message="Schema matches expected columns."
            )
        self.results.append(result)
        logger.info(f"[{result.status}] schema_drift_check — {result.message}")
        return result

    # ── Check 6: KPI Logic Validation ───────────────────────────────────────
    def check_kpi_logic(self, revenue_col: str, quantity_col: str, unit_price_col: str) -> CheckResult:
        """
        Validate that REVENUE = QUANTITY × UNIT_PRICE within tolerance.
        Mirrors KPI logic checks done for Power BI dashboards at Hotwire.
        """
        required = [revenue_col, quantity_col, unit_price_col]
        if not all(c in self.df.columns for c in required):
            result = CheckResult("kpi_logic_check", "WARNING", f"Required columns for KPI check not found: {required}")
            self.results.append(result)
            return result

        expected = self.df[quantity_col] * self.df[unit_price_col]
        diff = (self.df[revenue_col] - expected).abs()
        mismatches = (diff > 0.01).sum()    # 1 cent tolerance

        if mismatches > 0:
            result = CheckResult(
                check_name="kpi_logic_check",
                status="FAILED",
                message=f"{mismatches:,} rows where REVENUE ≠ QUANTITY × UNIT_PRICE",
                rows_affected=mismatches,
                details={"max_diff": float(diff.max()), "mean_diff": float(diff.mean())}
            )
        else:
            result = CheckResult(
                check_name="kpi_logic_check",
                status="PASSED",
                message="KPI logic validated: REVENUE = QUANTITY × UNIT_PRICE for all rows."
            )
        self.results.append(result)
        logger.info(f"[{result.status}] kpi_logic_check — {result.message}")
        return result

    # ── Reconciliation Report ────────────────────────────────────────────────
    def generate_report(self) -> pd.DataFrame:
        """Return all check results as a DataFrame for logging or dashboarding."""
        report = pd.DataFrame([
            {
                "check_name":    r.check_name,
                "status":        r.status,
                "message":       r.message,
                "rows_affected": r.rows_affected,
                "timestamp":     r.timestamp,
            }
            for r in self.results
        ])
        passed  = (report["status"] == "PASSED").sum()
        failed  = (report["status"] == "FAILED").sum()
        warning = (report["status"] == "WARNING").sum()
        logger.info("=" * 60)
        logger.info(f"RECONCILIATION REPORT — {self.source_name}")
        logger.info(f"  PASSED: {passed}  |  FAILED: {failed}  |  WARNING: {warning}")
        logger.info("=" * 60)
        return report


# ─── Example Usage ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Simulate a Bronze-layer DataFrame (replace with actual Snowflake query result)
    sample_data = {
        "ORDER_ID":        [1, 2, 2, 4, 5, None, 7, 8, 9, 10],
        "CUSTOMER_ID":     ["C1","C2","C2","C4","C5","C6","C7","C8","C9","C10"],
        "ORDER_DATE":      pd.date_range("2024-01-01", periods=10, freq="D"),
        "AMOUNT":          [100, 200, 200, -50, 300, 400, 250, 99999, 180, 220],
        "QUANTITY":        [2, 4, 4, 1, 6, 8, 5, 2, 3, 4],
        "UNIT_PRICE":      [50, 50, 50, 50, 50, 50, 50, 50, 60, 55],
    }
    df = pd.DataFrame(sample_data)

    engine = DataQualityEngine(df, source_name="BRONZE.RAW_ORDERS")

    engine.check_null_rate(columns=["ORDER_ID", "CUSTOMER_ID", "AMOUNT"], threshold=0.05)
    engine.check_duplicates(key_columns=["ORDER_ID"], threshold=0.01)
    engine.check_record_reconciliation(
        source_a_count=10, source_b_count=9,
        source_a_name="SolarWinds", source_b_name="AMS",
        tolerance_pct=0.02
    )
    engine.check_value_ranges(column="AMOUNT", min_val=0, max_val=10000)
    engine.check_schema_drift(expected_columns=["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "AMOUNT"])
    engine.check_kpi_logic(revenue_col="AMOUNT", quantity_col="QUANTITY", unit_price_col="UNIT_PRICE")

    report = engine.generate_report()
    print("\n", report.to_string(index=False))
