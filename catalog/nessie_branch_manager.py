#!/usr/bin/env python3
"""
Project Nessie Git-Style Branch & Transaction Manager
Module 1: Storage Layer & Git-Style Iceberg Catalog

Enables Git-style operations on Iceberg tables via Project Nessie REST API:
- Branch creation (e.g. feature/q3-reconcile, etl-batch-YYYYMMDD)
- Branch listing & commit log inspection (audit trail)
- Atomic fast-forward merges into main
- Tag creation for regulatory audit milestones
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request


class NessieClient:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or os.getenv("NESSIE_URI") or "http://localhost:19120/api/v1").rstrip("/")

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                content = resp.read().decode("utf-8")
                return json.loads(content) if content else {}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Nessie API Error [{e.code}] {e.reason}: {err_body}")
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to Nessie at {url}: {e.reason}")

    def get_default_branch(self) -> Dict[str, Any]:
        """Fetch the default branch reference (usually 'main')."""
        return self._request("GET", "/trees/tree")

    def list_references(self) -> List[Dict[str, Any]]:
        """List all branches and tags."""
        res = self._request("GET", "/trees")
        return res.get("references", [])

    def get_reference(self, ref_name: str) -> Dict[str, Any]:
        """Get details (including hash) of a specific branch or tag."""
        encoded_name = urllib.parse.quote(ref_name, safe="")
        return self._request("GET", f"/trees/tree/{encoded_name}")

    def create_branch(self, branch_name: str, source_ref: str = "main") -> Dict[str, Any]:
        """Create a new branch from a source reference."""
        source = self.get_reference(source_ref)
        payload = {
            "type": "BRANCH",
            "name": branch_name,
            "hash": source.get("hash")
        }
        source_hash = source.get("hash")
        return self._request("POST", f"/trees/tree?sourceRefName={urllib.parse.quote(source_ref, safe='')}&targetHash={source_hash}", payload)

    def merge_branch(self, from_branch: str, into_branch: str = "main") -> Dict[str, Any]:
        """Atomically merge from_branch into into_branch (e.g. fast-forward)."""
        target = self.get_reference(into_branch)
        source = self.get_reference(from_branch)
        payload = {
            "fromRefName": from_branch,
            "fromHash": source.get("hash")
        }
        target_name_enc = urllib.parse.quote(into_branch, safe="")
        target_hash = target.get("hash")
        return self._request("POST", f"/trees/branch/{target_name_enc}/merge?expectedHash={target_hash}", payload)

    def delete_branch(self, branch_name: str) -> Dict[str, Any]:
        """Delete an ephemeral branch after merge."""
        ref = self.get_reference(branch_name)
        ref_name_enc = urllib.parse.quote(branch_name, safe="")
        target_hash = ref.get("hash")
        return self._request("DELETE", f"/trees/branch/{ref_name_enc}?expectedHash={target_hash}")

    def get_commit_log(self, ref_name: str = "main", limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch commit history for a branch."""
        ref_name_enc = urllib.parse.quote(ref_name, safe="")
        res = self._request("GET", f"/trees/tree/{ref_name_enc}/log?maxRecords={limit}")
        return res.get("logEntries", [])


def main():
    parser = argparse.ArgumentParser(description="IceFlow Core Nessie Git-Style Branch Manager")
    parser.add_argument("--url", help="Nessie REST API URL", default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list
    subparsers.add_parser("list", help="List all branches and tags")

    # create
    create_p = subparsers.add_parser("create-branch", help="Create a new branch")
    create_p.add_argument("branch", help="Name of new branch")
    create_p.add_argument("--source", default="main", help="Source branch (default: main)")

    # merge
    merge_p = subparsers.add_parser("merge", help="Merge a branch into target")
    merge_p.add_argument("from_branch", help="Source branch to merge from")
    merge_p.add_argument("--into", default="main", help="Target branch (default: main)")

    # log
    log_p = subparsers.add_parser("log", help="Show commit history for branch")
    log_p.add_argument("--branch", default="main", help="Branch name (default: main)")
    log_p.add_argument("--limit", type=int, default=10, help="Max commit entries")

    # delete
    del_p = subparsers.add_parser("delete-branch", help="Delete a merged branch")
    del_p.add_argument("branch", help="Branch name to delete")

    args = parser.parse_args()
    client = NessieClient(base_url=args.url)

    try:
        if args.command == "list":
            refs = client.list_references()
            print(f"--- Nessie Catalog References ({len(refs)} found) ---")
            for r in refs:
                print(f"[{r.get('type')}] {r.get('name')} -> Commit Hash: {r.get('hash')}")

        elif args.command == "create-branch":
            created = client.create_branch(args.branch, source_ref=args.source)
            print(f"Successfully created branch '{args.branch}' from '{args.source}'. Details: {created}")

        elif args.command == "merge":
            merged = client.merge_branch(args.from_branch, into_branch=args.into)
            print(f"Atomically merged branch '{args.from_branch}' into '{args.into}'.")

        elif args.command == "log":
            logs = client.get_commit_log(ref_name=args.branch, limit=args.limit)
            print(f"--- Commit Log for branch '{args.branch}' (Showing {len(logs)}) ---")
            for entry in logs:
                meta = entry.get("commitMeta", {})
                print(f"Hash: {meta.get('hash')}")
                print(f"  Author: {meta.get('author')}")
                print(f"  Message: {meta.get('message')}")
                print(f"  Committed At: {meta.get('commitTime')}")
                print("-" * 50)

        elif args.command == "delete-branch":
            client.delete_branch(args.branch)
            print(f"Branch '{args.branch}' deleted successfully.")

    except Exception as e:
        print(f"[ERROR] Nessie Branch Manager: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
