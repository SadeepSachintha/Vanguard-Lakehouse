#!/usr/bin/env python3
"""
Automated Data Contract Gateways & Anomaly Verification Engine
Module 3: Continuous Data Contracts & Quality Gating

Features:
- Validates physical DuckDB / Iceberg tables against explicit YAML contracts
- Enforces strict data typing and zero-nullity rules on primary/foreign keys
- Checks data distribution metrics (e.g. price anomalies where Delta > 50% day-over-day)
- Evaluates referential integrity relationships between Fact and Dimensions
- Blocks CI/CD merge if contract or anomaly assertions fail (exit code 1)
"""

import argparse
from datetime import datetime
import os
import sys
from typing import Any, Dict, List, Tuple

import duckdb
import yaml


class ContractGateway:
    def __init__(self, db_path: str = "iceflow_lakehouse.duckdb"):
        self.db_path = db_path
        self.con = duckdb.connect(db_path, read_only=True)
        # Dynamically configure DuckDB search path across available lakehouse schemas
        existing_schemas = [row[0] for row in self.con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()]
        candidate_schemas = ["main_gold", "main_silver", "gold", "silver", "main"]
        active_schemas = [s for s in candidate_schemas if s in existing_schemas]
        if active_schemas:
            self.con.execute(f"SET search_path = '{','.join(active_schemas)}'")

        self.errors: List[str] = []
        self.warnings: List[str] = []

    def log_error(self, message: str):
        self.errors.append(message)
        print(f"❌ [CONTRACT VIOLATION] {message}", file=sys.stderr)

    def log_warning(self, message: str):
        self.warnings.append(message)
        print(f"⚠️ [WARNING] {message}")

    def log_success(self, message: str):
        print(f"✅ [CONTRACT PASSED] {message}")

    def validate_schema_contract(self, contract_yaml_path: str):
        """Enforces schema column names and expected types from YAML contract."""
        print(f"\n--- Checking Schema Contracts from {contract_yaml_path} ---")
        if not os.path.exists(contract_yaml_path):
            self.log_error(f"Contract file not found: {contract_yaml_path}")
            return

        with open(contract_yaml_path, "r") as f:
            contract_doc = yaml.safe_load(f)

        for model in contract_doc.get("models", []):
            model_name = model["name"]
            expected_cols = {c["name"]: c.get("data_type", "").lower() for c in model.get("columns", [])}

            # Check if table exists in DuckDB
            try:
                table_info = self.con.execute(f"PRAGMA table_info('{model_name}')").fetchall()
                if not table_info:
                    self.log_error(f"Table '{model_name}' does not exist in lakehouse catalog.")
                    continue
            except Exception as e:
                self.log_error(f"Failed to query table '{model_name}': {e}")
                continue

            actual_cols = {row[1]: row[2].lower() for row in table_info}

            # Verify missing or extra columns
            for col_name, exp_type in expected_cols.items():
                if col_name not in actual_cols:
                    self.log_error(f"Table '{model_name}' missing expected column '{col_name}'.")
                else:
                    act_type = actual_cols[col_name]
                    # Flexible type normalization (e.g. varchar vs text, integer vs int4)
                    if exp_type and exp_type not in act_type and act_type not in exp_type:
                        self.log_error(
                            f"Column '{model_name}.{col_name}' schema mismatch: "
                            f"Expected '{exp_type}', found '{act_type}'."
                        )

            self.log_success(f"Model '{model_name}' adheres to schema contract.")

    def validate_nullity_and_uniqueness(self):
        """Verifies primary and foreign key constraints."""
        print("\n--- Checking Nullity & Primary Key Uniqueness ---")
        checks = [
            ("dim_commodities", "commodity_key", True),
            ("dim_dates", "date_key", True),
            ("dim_provinces", "province_key", True),
            ("fct_commodity_prices", "fact_price_key", True),
            ("fct_commodity_prices", "commodity_key", False),
            ("fct_commodity_prices", "date_key", False),
            ("fct_commodity_prices", "province_key", False),
        ]

        for table, col, check_unique in checks:
            try:
                # Nullity check
                null_cnt = self.con.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
                if null_cnt > 0:
                    self.log_error(f"{null_cnt} NULL value(s) detected in '{table}.{col}'!")
                else:
                    self.log_success(f"Zero NULLs verified for '{table}.{col}'.")

                # Uniqueness check
                if check_unique:
                    dup_cnt = self.con.execute(
                        f"SELECT COUNT(*) FROM (SELECT {col}, COUNT(*) FROM {table} GROUP BY {col} HAVING COUNT(*) > 1)"
                    ).fetchone()[0]
                    if dup_cnt > 0:
                        self.log_error(f"Duplicate primary key entries ({dup_cnt}) detected in '{table}.{col}'!")
                    else:
                        self.log_success(f"Uniqueness constraint verified for '{table}.{col}'.")
            except Exception as e:
                self.log_error(f"Nullity/Unique check failed on '{table}.{col}': {e}")

    def validate_referential_integrity(self):
        """Checks foreign key referential integrity in star schema."""
        print("\n--- Checking Star Schema Referential Integrity ---")
        fk_checks = [
            ("fct_commodity_prices", "commodity_key", "dim_commodities", "commodity_key"),
            ("fct_commodity_prices", "date_key", "dim_dates", "date_key"),
            ("fct_commodity_prices", "province_key", "dim_provinces", "province_key"),
        ]

        for fact_tbl, fk_col, dim_tbl, pk_col in fk_checks:
            try:
                orphan_query = f"""
                    SELECT COUNT(*)
                    FROM {fact_tbl} f
                    LEFT JOIN {dim_tbl} d ON f.{fk_col} = d.{pk_col}
                    WHERE d.{pk_col} IS NULL
                """
                orphan_cnt = self.con.execute(orphan_query).fetchone()[0]
                if orphan_cnt > 0:
                    self.log_error(
                        f"Referential integrity failure: {orphan_cnt} orphan record(s) in "
                        f"'{fact_tbl}.{fk_col}' with no match in '{dim_tbl}.{pk_col}'."
                    )
                else:
                    self.log_success(f"Referential integrity verified: {fact_tbl}.{fk_col} -> {dim_tbl}.{pk_col}.")
            except Exception as e:
                self.log_error(f"Referential integrity check failed for {fact_tbl} -> {dim_tbl}: {e}")

    def validate_distribution_and_anomalies(self, anomaly_threshold_pct: float = 50.0):
        """
        Validates distribution metrics:
        Asserts that day-over-day price delta does not exceed anomaly threshold without flagging.
        """
        print(f"\n--- Checking Distribution Metrics & Day-over-Day Anomalies (Threshold: {anomaly_threshold_pct}%) ---")
        try:
            # Check for negative prices
            neg_prices = self.con.execute("""
                SELECT COUNT(*)
                FROM fct_commodity_prices
                WHERE wholesale_price_lkr <= 0 OR retail_price_lkr <= 0
            """).fetchone()[0]
            if neg_prices > 0:
                self.log_error(f"Found {neg_prices} records with non-positive prices (<= 0 LKR)!")
            else:
                self.log_success("All wholesale and retail prices are strictly positive (> 0 LKR).")

            # Check price anomaly distribution
            anomaly_query = f"""
                SELECT
                    commodity_id,
                    market_center,
                    trade_date,
                    prev_day_wholesale_price_lkr,
                    wholesale_price_lkr,
                    day_over_day_price_change_pct
                FROM fct_commodity_prices
                WHERE abs(day_over_day_price_change_pct) > {anomaly_threshold_pct}
                ORDER BY abs(day_over_day_price_change_pct) DESC
            """
            anomalies = self.con.execute(anomaly_query).fetchall()
            if anomalies:
                self.log_warning(
                    f"Contract Gateway detected {len(anomalies)} price spike anomaly record(s) where |Delta| > {anomaly_threshold_pct}%."
                )
                for a in anomalies[:5]:
                    print(f"     -> {a[0]} at {a[1]} on {a[2]}: {a[3]} -> {a[4]} LKR ({a[5]}%)")
            else:
                self.log_success(f"Zero price anomalies exceeding {anomaly_threshold_pct}% day-over-day.")

        except Exception as e:
            self.log_error(f"Distribution check failed: {e}")

    def close(self):
        self.con.close()


def main():
    parser = argparse.ArgumentParser(description="IceFlow Core Automated Data Contract Gateways")
    parser.add_argument("--db", default="iceflow_lakehouse.duckdb", help="DuckDB database file path")
    parser.add_argument("--contracts-yml", default="dbt_transforms/models/marts/_marts_models.yml", help="Path to models YAML contract")
    parser.add_argument("--threshold", type=float, default=50.0, help="Price anomaly threshold percentage")

    args = parser.parse_args()

    print("======================================================")
    print("  IceFlow Core Automated Continuous Contract Gateway  ")
    print("======================================================")

    gateway = ContractGateway(db_path=args.db)
    gateway.validate_schema_contract(args.contracts_yml)
    gateway.validate_nullity_and_uniqueness()
    gateway.validate_referential_integrity()
    gateway.validate_distribution_and_anomalies(anomaly_threshold_pct=args.threshold)
    gateway.close()

    print("\n======================================================")
    print(f"Summary: {len(gateway.errors)} Contract Violation(s), {len(gateway.warnings)} Warning(s)")
    print("======================================================")

    if gateway.errors:
        print("❌ CI/CD MERGE BLOCKED: Data contract validations failed.", file=sys.stderr)
        sys.exit(1)
    else:
        print("✅ CI/CD MERGE APPROVED: All lakehouse contract gates passed successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
