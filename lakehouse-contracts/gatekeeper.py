#!/usr/bin/env python3
"""
Vanguard Lakehouse Enterprise Contract Gatekeeper
Module: lakehouse-contracts/gatekeeper.py

Performs contract assertion gatekeeping for ingestion pipelines, dbt transformations,
and CI/CD merge gating based on machine-readable Open Data Contracts (ODCS).
"""

import argparse
from datetime import datetime, timezone
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import duckdb


class LakehouseContractValidator:
    def __init__(
        self,
        contract_spec_path: str = "lakehouse-contracts/schemas/stg_cbsl_commodity_prices.contract.json",
        db_path: str = "dbt_transforms/iceflow_lakehouse.duckdb",
    ):
        self.contract_spec_path = contract_spec_path
        self.db_path = db_path

        with open(contract_spec_path, "r") as f:
            self.contract: Dict[str, Any] = json.load(f)

        self.con = duckdb.connect(db_path, read_only=True)
        # Search path
        schemas = [r[0] for r in self.con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()]
        candidates = ["main_silver", "silver", "main_gold", "gold", "main"]
        active = [s for s in candidates if s in schemas]
        if active:
            self.con.execute(f"SET search_path = '{','.join(active)}'")

        self.errors: List[str] = []
        self.passed: List[str] = []

    def validate_schema(self, table_name: str = "stg_cbsl_commodity_prices"):
        """Validates that table columns, required fields, and types adhere to the contract."""
        fields = self.contract["models"][table_name]["fields"]
        table_info = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        actual_cols = {row[1]: row[2].lower() for row in table_info}

        for col_name, col_meta in fields.items():
            if col_name not in actual_cols:
                self.errors.append(f"Contract Schema Failure: Required column '{col_name}' missing in '{table_name}'")
                continue

            actual_type = actual_cols[col_name]
            expected_type = col_meta["type"].lower()

            # Flexible type validation (e.g. varchar vs string, decimal(10,2) vs decimal)
            if "string" in expected_type:
                expected_type = "varchar"
            if "decimal" in expected_type:
                expected_type = "decimal"

            if expected_type not in actual_type:
                self.errors.append(
                    f"Contract Type Failure: Column '{col_name}' expected '{col_meta['type']}', got '{actual_type}'"
                )
            else:
                self.passed.append(f"Column '{col_name}' typing verified ({actual_type})")

    def validate_primary_key(self, table_name: str = "stg_cbsl_commodity_prices"):
        """Ensures surrogate primary keys are strictly non-null and unique."""
        fields = self.contract["models"][table_name]["fields"]
        pk_cols = [k for k, v in fields.items() if v.get("primaryKey")]

        for pk in pk_cols:
            null_count = self.con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {pk} IS NULL").fetchone()[0]
            if null_count > 0:
                self.errors.append(f"Contract PK Violation: Found {null_count} NULLs in primary key '{pk}'")
            else:
                self.passed.append(f"Primary key '{pk}' nullity constraint verified (0 nulls)")

            dup_count = self.con.execute(
                f"SELECT COUNT(*) FROM (SELECT {pk}, COUNT(*) FROM {table_name} GROUP BY {pk} HAVING COUNT(*) > 1)"
            ).fetchone()[0]
            if dup_count > 0:
                self.errors.append(f"Contract PK Violation: Found {dup_count} duplicate primary key(s) in '{pk}'")
            else:
                self.passed.append(f"Primary key '{pk}' uniqueness constraint verified (0 duplicates)")

    def validate_quality_rules(self, table_name: str = "stg_cbsl_commodity_prices"):
        """Executes explicit quality rules declared in the contract."""
        rules = self.contract.get("quality", [])
        for rule in rules:
            rule_name = rule["name"]
            if rule["type"] == "customSql":
                try:
                    count = self.con.execute(rule["query"]).fetchone()[0]
                    expected = rule.get("mustBe", 0)
                    if count != expected:
                        self.errors.append(
                            f"Quality Rule Violation [{rule_name}]: Expected {expected} breaches, found {count}"
                        )
                    else:
                        self.passed.append(f"Quality rule '{rule_name}' passed ({rule['description']})")
                except Exception as exc:
                    self.errors.append(
                        f"Quality Rule Execution Failure [{rule_name}]: {exc}"
                    )

    def run(self) -> int:
        print("==================================================================")
        print(f"  Vanguard Lakehouse ODCS Data Contract Gatekeeper Execution     ")
        print("==================================================================")
        print(f"Contract ID : {self.contract.get('id')}")
        print(f"Version     : {self.contract.get('info', {}).get('version')}")
        print(f"Tier/Target : {self.contract.get('dataset', {}).get('tier')} ({self.contract.get('dataset', {}).get('physicalName')})")
        print("==================================================================\n")

        self.validate_schema()
        self.validate_primary_key()
        self.validate_quality_rules()

        print(f"Results: {len(self.passed)} Checks Passed, {len(self.errors)} Contract Breaches\n")
        for p in self.passed:
            print(f"  ✅ {p}")
        for e in self.errors:
            print(f"  ❌ {e}")

        print("==================================================================")
        if self.errors:
            print("🛑 GATEKEEPER STATUS: CONTRACT BREACHED - MERGE BLOCKED", file=sys.stderr)
            return 1
        else:
            print("🎉 GATEKEEPER STATUS: CONTRACT COMPLIANT - MERGE APPROVED")
            return 0


def main():
    parser = argparse.ArgumentParser(description="Vanguard Lakehouse Contract Validator")
    parser.add_argument("--contract", default="lakehouse-contracts/schemas/stg_cbsl_commodity_prices.contract.json")
    parser.add_argument("--db", default="dbt_transforms/iceflow_lakehouse.duckdb")
    args = parser.parse_args()

    validator = LakehouseContractValidator(contract_spec_path=args.contract, db_path=args.db)
    sys.exit(validator.run())


if __name__ == "__main__":
    main()
