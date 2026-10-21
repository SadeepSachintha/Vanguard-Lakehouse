#!/usr/bin/env python3
"""
Vanguard Lakehouse: Ingestion Quarantine & Dead-Letter Queue (DLQ) Inspector
Enables inspecting, diagnosing, and replaying quarantined ingestion payloads.
"""

import argparse
from datetime import datetime
import json
import os
import sys
from typing import Any, Dict, List

import pandas as pd
from tabulate import tabulate


class QuarantineInspector:
    def __init__(self, quarantine_base_dir: str = "bronze_data/quarantine"):
        self.quarantine_base_dir = quarantine_base_dir

    def list_records(self, dataset: Optional[str] = None):
        """Lists quarantined records across datasets."""
        if not os.path.exists(self.quarantine_base_dir):
            print(f"ℹ️  Quarantine directory empty or not found: {self.quarantine_base_dir}")
            return

        all_records = []
        datasets = [dataset] if dataset else os.listdir(self.quarantine_base_dir)

        for d in datasets:
            d_path = os.path.join(self.quarantine_base_dir, d)
            if not os.path.isdir(d_path):
                continue

            jsonl_file = os.path.join(d_path, "quarantine.jsonl")
            if os.path.exists(jsonl_file):
                with open(jsonl_file, "r") as f:
                    for line in f:
                        if line.strip():
                            rec = json.loads(line.strip())
                            all_records.append({
                                "Quarantine ID": rec.get("quarantine_id", "")[:12] + "...",
                                "Full ID": rec.get("quarantine_id"),
                                "Dataset": rec.get("dataset_name"),
                                "Quarantined At": rec.get("quarantined_at"),
                                "Violating Fields": ", ".join(rec.get("violating_fields", [])),
                                "Failure Reason": rec.get("failure_reason", "")[:50] + "..."
                            })

        print(f"\n=======================================================")
        print(f"    QUARANTINE DEAD-LETTER QUEUE (DLQ) RECORDS         ")
        print(f"=======================================================")
        if not all_records:
            print("✅ Zero records in quarantine. Ingestion stream is 100% healthy.")
            return

        display_df = pd.DataFrame(all_records).drop(columns=["Full ID"])
        print(tabulate(display_df, headers="keys", tablefmt="fancy_grid", showindex=False))
        print(f"Total Quarantined Items: {len(all_records)}\n")

    def inspect_record(self, quarantine_id: str):
        """Displays full diagnostic details and raw payload for a specific quarantine ID."""
        if not os.path.exists(self.quarantine_base_dir):
            print("Quarantine directory does not exist.")
            return

        for root, _, files in os.walk(self.quarantine_base_dir):
            if "quarantine.jsonl" in files:
                file_path = os.path.join(root, "quarantine.jsonl")
                with open(file_path, "r") as f:
                    for line in f:
                        rec = json.loads(line.strip())
                        if rec.get("quarantine_id") == quarantine_id or rec.get("quarantine_id", "").startswith(quarantine_id):
                            print(f"\n=======================================================")
                            print(f"  QUARANTINE RECORD DIAGNOSTIC: {rec.get('quarantine_id')}  ")
                            print(f"=======================================================")
                            print(f"Dataset Name     : {rec.get('dataset_name')}")
                            print(f"Quarantined At   : {rec.get('quarantined_at')}")
                            print(f"Violating Fields : {rec.get('violating_fields')}")
                            print(f"Failure Reason   : {rec.get('failure_reason')}")
                            print("\nRaw Telemetry Payload:")
                            try:
                                formatted_json = json.dumps(json.loads(rec.get("raw_payload", "{}")), indent=2)
                                print(formatted_json)
                            except Exception:
                                print(rec.get("raw_payload"))
                            print("=======================================================\n")
                            return

        print(f"❌ Record with ID '{quarantine_id}' not found in quarantine.")


def main():
    parser = argparse.ArgumentParser(description="Vanguard Lakehouse Ingestion Quarantine DLQ Inspector")
    parser.add_argument("--dir", default="bronze_data/quarantine", help="Quarantine directory path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list
    list_p = subparsers.add_parser("list", help="List quarantined records")
    list_p.add_argument("--dataset", default=None, help="Filter by dataset name")

    # inspect
    inspect_p = subparsers.add_parser("inspect", help="Inspect detailed payload of a quarantined record")
    inspect_p.add_argument("id", help="Quarantine ID (or prefix)")

    args = parser.parse_args()
    inspector = QuarantineInspector(quarantine_base_dir=args.dir)

    if args.command == "list":
        inspector.list_records(dataset=args.dataset)
    elif args.command == "inspect":
        inspector.inspect_record(quarantine_id=args.id)


if __name__ == "__main__":
    main()
