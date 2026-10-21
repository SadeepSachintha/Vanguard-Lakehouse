# IceFlow Core: Enterprise Open Data Lakehouse

[![Lakehouse CI](https://github.com/SadeepSachintha/Vanguard-Lakehouse/actions/workflows/dbt-ci-contract-check.yml/badge.svg)](https://github.com/SadeepSachintha/Vanguard-Lakehouse/actions/workflows/dbt-ci-contract-check.yml)
[![Docker Build](https://github.com/SadeepSachintha/Vanguard-Lakehouse/actions/workflows/docker-build.yml/badge.svg)](https://github.com/SadeepSachintha/Vanguard-Lakehouse/actions/workflows/docker-build.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

An open data lakehouse architecture purpose-built to eliminate silent data corruption, full-table scans, lack of ACID guarantees, and spaghetti SQL scripts in open macroeconomic and commodity data pipelines.

---

## 1. Problem Statement & Modern Lakehouse Solution

Traditional data platforms frequently dump raw, unvalidated JSON/CSV payloads directly into relational databases (PostgreSQL) or unversioned object storage (S3/MinIO). In production, this causes:

1. **Silent Data Corruption & Schema Drift:** Upstream API alterations (e.g., changes in central bank date formats or numeric float typing) silently cascade into downstream dashboards without contract barriers.
2. **Expensive Full-Table Scans & High Storage Drag:** Querying millions of historical observations requires unpartitioned sweeps across massive raw object dumps.
3. **Lack of ACID Guarantees & Audit Trails:** Concurrent writer pipelines produce dirty reads and write skews, with zero point-in-time "Time Travel" rollback capabilities for regulatory compliance.
4. **Spaghetti SQL Transformations:** Business logic and analytical metric aggregations remain tangled inside monolithic Python scripts rather than modular, tested dimensional models.

### The IceFlow Core Paradigm

```
┌────────────────────────────────────────────────────────┐
│      External Data Ingestors (Airflow / Python)        │
│   Ingests: CBSL Forex, Market Commodities, Weather     │
└───────────────────────────┬────────────────────────────┘
                            │ (Raw Bronze Writes: JSON / Parquet)
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    MinIO S3-Compatible Object Store                     │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │             Project Nessie / Iceberg REST Catalog               │   │
│   │  - ACID Transactions (Concurrent Writer Isolation)              │   │
│   │  - Git-style Branching (`main`, `feature/q3-reconcile`)         │   │
│   │  - Time-Travel Snapshots & Zero-Copy Rollbacks                  │   │
│   └───────────────────────────────▲─────────────────────────────────┘   │
│                                   │                                     │
└───────────────────────────────────┼─────────────────────────────────────┘
                                    │
          ┌─────────────────────────┴────────────────────────┐
          ▼                                                  ▼
┌──────────────────────────────────────┐   ┌──────────────────────────────────────┐
│       dbt-core Modeling Engine       │   │   Automated Data Contract Gateways   │
│ - Silver: Cleansing & Deduplication  │   │ - Great Expectations / Soda Core     │
│ - Gold: Dimensional Star Schema      │   │ - Schema Typing & Nullity Assertion  │
│ - Snapshots: SCD Type 2 Histories    │   │ - CI/CD Merge Blocking               │
└──────────────────┬───────────────────┘   └──────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│   DuckDB / PyIceberg Analytics OLAP  │ ◄── Sub-second SQL Querying & Audit Logs
└──────────────────────────────────────┘
```

---

## 2. Core Functional Modules

### Module 1: Storage Layer & Git-Style Iceberg Catalog
* **MinIO Object Lake:** Dedicated S3 buckets (`bronze`, `silver`, `gold`, `warehouse`) provisioned with path-style access.
* **Apache Iceberg Table Format:** Tables feature **hidden partitioning** (`partition by month(trade_date)`), eliminating partition management bugs and enabling pruning during OLAP queries.
* **Project Nessie Integration:** Serves as the transactional Git-like catalog. Batch ETL pipelines write to an isolated branch (e.g. `feature/q3-reconcile`), validate contract assertions, and atomically fast-forward merge into `main` without downtime or dirty reads.
* **Metadata Maintenance:** Automated small-file compaction (bin-packing data files) and orphan snapshot cleanup routines prevent metadata bloat.

### Module 2: Modular Transformation via dbt-core
* **Staging / Silver Layer (`models/staging`):**
  * `stg_cbsl_forex`: Cleans raw CBSL forex records, standardizes ISO currency codes, parses timestamps, and casts numeric rates to explicit financial decimals (`DECIMAL(18, 4)`).
  * `stg_market_commodities`: Cleans commodity observations, standardizes commodity IDs (`RICE_SAMBA`, `POTATO_LOCAL`), parses trade dates, and generates a deterministic **SHA-256 surrogate key** (`macros/generate_surrogate_key.sql`) to deduplicate redundant upstream scrapes.
* **Marts / Gold Layer (`models/marts`):**
  * `fct_commodity_prices`: Star-schema fact table computing retail-wholesale spreads, retail markup percentages, and rolling day-over-day price deltas.
  * `dim_commodities`: Conformed dimension of food items, commodity categories, and trading units.
  * `dim_dates`: Calendar dimension with `date_key` (YYYYMMDD integer), calendar attributes, and weekend indicators.
  * `dim_provinces`: Dimension of Sri Lankan wholesale markets, economic centers (Pettah, Dambulla, etc.), and agro-climatic zones.
* **Slowly Changing Dimensions (SCD Type 2):**
  * `snapshots/snap_commodity_grades.sql`: Tracks item metadata alterations (e.g., changes in official retail grade classifications or reporting market hubs) over time with `dbt_valid_from` and `dbt_valid_to` columns.

### Module 3: Continuous Data Contracts & Quality Gating
* **Schema Contracts:** Explicit `contract: {enforced: true}` blocks in dbt YAML files. If an upstream scraper changes a column type (e.g. integer to string), compilation fails immediately.
* **dbt Quality Assertions:** Native tests validating `unique`, `not_null`, referential integrity (`relationships`) between fact foreign keys and dimension primary keys, plus custom tests (`assert_non_negative_prices.sql`).
* **Continuous Contract Gateways (`contracts/contract_gateways.py`):**
  * Evaluates schema typing and nullity assertions against published contracts.
  * Validates data distribution metrics: Flags price anomalies where day-over-day delta $\Delta > 50\%$ without explicit anomaly classification.
  * Blocks CI/CD pull request merges if contract violations or unflagged anomalies are found.

### Module 4: High-Performance Analytical Serving & Time-Travel
* **Sub-Second OLAP:** Directly queries Iceberg Gold tables via DuckDB and PyIceberg in milliseconds without needing expensive cloud data warehouse compute fees.
* **Time-Travel Audit Verification:** Exposes CLI and programmatic API endpoints demonstrating historical point-in-time audit queries:
  ```sql
  SELECT * FROM gold_commodity_prices 
  FOR SYSTEM_TIME AS OF '2026-10-01 00:00:00 UTC'
  WHERE commodity_id = 'RICE_SAMBA';
  ```

---

## 3. Repository File Structure

```text
iceflow-core/
├── .github/
│   └── workflows/
│       ├── dbt-ci-contract-check.yml   # Continuous contract verification & anomaly gating
│       └── docker-build.yml             # Docker container validation workflow
├── docker-compose.yml                   # MinIO, MinIO Init, Nessie Catalog, Ingestion, dbt
├── Makefile                             # Orchestration commands
├── catalog/
│   ├── nessie-config.env                # Nessie and S3 credentials
│   └── nessie_branch_manager.py         # Git-style branch creation, commit logs, merges
├── ingestion/
│   ├── Dockerfile                       # Containerized ingestion worker
│   ├── requirements.txt                 # PyIceberg, DuckDB, Boto3, Pydantic dependencies
│   ├── cbsl_scraper.py                  # CBSL Forex, Commodities & Weather scraper
│   └── write_to_iceberg.py              # Bronze -> Iceberg format with hidden partitioning
├── dbt_transforms/
│   ├── dbt_project.yml                  # dbt project configuration
│   ├── profiles.yml                     # DuckDB & S3 profile
│   ├── macros/
│   │   ├── generate_surrogate_key.sql   # SHA-256 row hashing macro
│   │   └── read_bronze_source.sql       # Resilient parquet & warehouse source reader
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml             # Raw Bronze declarations
│   │   │   ├── stg_cbsl_forex.sql       # Standardized forex ticks
│   │   │   └── stg_market_commodities.sql # Standardized commodity prices
│   │   └── marts/
│   │       ├── _marts_models.yml        # Enforced data contracts & constraints
│   │       ├── dim_commodities.sql      # Conformed commodity dimension
│   │       ├── dim_dates.sql            # Calendar date dimension
│   │       ├── dim_provinces.sql        # Market centers & provinces dimension
│   │       └── fct_commodity_prices.sql # Star-schema fact table
│   ├── snapshots/
│   │   └── snap_commodity_grades.sql    # SCD Type 2 history snapshot
│   └── tests/
│       ├── assert_non_negative_prices.sql # Singular price non-negativity test
│       └── assert_price_anomaly_threshold.sql # Price volatility test
├── contracts/
│   ├── contract_gateways.py             # Contract verification & CI merge blocker
│   └── soda_checks.yml                  # Soda Core / Great Expectations specifications
└── analytics/
    └── query_time_travel_duckdb.py      # Sub-second OLAP & Time-Travel audit engine
```

---

## 4. Quickstart Guide

### Option A: Local Python & DuckDB Execution (Fastest)

Run the full end-to-end pipeline locally with Python and DuckDB:

```bash
# 1. Install dependencies
pip install -r ingestion/requirements.txt
pip install dbt-core==1.8.0 dbt-duckdb==1.8.0 pyyaml tabulate

# 2. Run the full pipeline via Makefile
make pipeline
```

Or step-by-step:

```bash
# Step 1: Ingest CBSL Forex, Commodities & Weather into Bronze storage
python ingestion/cbsl_scraper.py --start-date 2026-08-01 --end-date 2026-10-21 --no-s3

# Step 2: Convert Bronze to Iceberg tables with month(trade_date) partitioning
python ingestion/write_to_iceberg.py --branch feature/q3-reconcile --maintenance

# Step 3: Run dbt Staging & Marts transformations
cd dbt_transforms
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt snapshot --profiles-dir .
cd ..

# Step 4: Validate Data Contracts and Quality Gateways
python contracts/contract_gateways.py \
  --db dbt_transforms/iceflow_lakehouse.duckdb \
  --contracts-yml dbt_transforms/models/marts/_marts_models.yml \
  --threshold 50.0

# Step 5: Execute Time-Travel Audit Query
python analytics/query_time_travel_duckdb.py \
  --db dbt_transforms/iceflow_lakehouse.duckdb \
  --commodity RICE_SAMBA \
  --as-of "2026-10-01 00:00:00 UTC"
```

---

### Option B: Docker Compose Full Stack

Spin up the containerized lakehouse stack:

```bash
# 1. Start MinIO S3 and Project Nessie Catalog
make up

# 2. Open MinIO Console in your browser:
#    URL: http://localhost:9001 (Credentials: admin / password123)
#    Buckets: bronze, silver, gold, warehouse

# 3. Open Project Nessie REST Catalog:
#    URL: http://localhost:19120/api/v1/config

# 4. Stop all services
make down
```

---

## 5. Demonstration: Time-Travel & Git-Style Branching

### Iceberg Time-Travel Audit Verification

Execute point-in-time historical queries across snapshots:

```bash
python analytics/query_time_travel_duckdb.py \
  --db dbt_transforms/iceflow_lakehouse.duckdb \
  --commodity RICE_SAMBA \
  --as-of "2026-10-01 00:00:00 UTC"
```

**Example Output:**
```text
=======================================================
  ICEFLOW TIME-TRAVEL AUDIT QUERY ENGINE
=======================================================
SQL Equivalent:
  SELECT * FROM gold_commodity_prices
  FOR SYSTEM_TIME AS OF '2026-10-01 00:00:00 UTC'
  WHERE commodity_id = 'RICE_SAMBA';
=======================================================

🎯 Matched Iceberg Snapshot ID : 1729482000000
   Snapshot Timestamp (UTC)   : 2026-10-01T00:00:00Z
   Commit Branch              : main
[TIME TRAVEL SUCCESS] Completed in 4.21 ms
+--------------+----------------+------------------+------------------------------------+-----------------+-----------------------+--------------------+-------------------------------+---------------------+
| trade_date   | commodity_id   | commodity_name   | market_center                      | quality_grade   |   wholesale_price_lkr |   retail_price_lkr |   retail_wholesale_spread_lkr |   retail_markup_pct |
+==============+================+==================+====================================+=================+=======================+====================+===============================+=====================+
| 2026-10-01   | RICE_SAMBA     | Samba Rice       | Pettah Wholesale Market            | Super Grade     |                246.50 |             295.80 |                         49.30 |               20.00 |
| 2026-10-01   | RICE_SAMBA     | Samba Rice       | Dambulla Dedicated Economic Centre | Grade A         |                228.30 |             273.96 |                         45.66 |               20.00 |
| 2026-10-01   | RICE_SAMBA     | Samba Rice       | Narahenpita Economic Centre        | Standard Grade  |                242.10 |             290.52 |                         48.42 |               20.00 |
+--------------+----------------+------------------+------------------------------------+-----------------+-----------------------+--------------------+-------------------------------+---------------------+
```

### Git-Style Nessie Branching Workflow

```bash
# 1. Create a dedicated reconciliation branch
python catalog/nessie_branch_manager.py create-branch feature/q3-reconcile --source main

# 2. Inspect commit history and audit trail
python catalog/nessie_branch_manager.py log --branch feature/q3-reconcile

# 3. Atomically fast-forward merge into main once contract gates pass
python catalog/nessie_branch_manager.py merge feature/q3-reconcile --into main
```

---

## 6. Continuous Data Contracts in CI/CD

IceFlow Core enforces a strict contract barrier on every GitHub Actions pull request:
1. **Schema Typing:** Validates column types against `_marts_models.yml`.
2. **Nullity & Keys:** Ensures primary and foreign keys contain 0 NULLs and 0 duplicate records.
3. **Referential Integrity:** Verifies all foreign keys in `fct_commodity_prices` link to valid dimension entries.
4. **Price Volatility Gate:** Fails the build if any day-over-day price jump exceeds $50\%$ without explicit anomaly classification.

If any test fails, CI immediately exits with code `1`, blocking merge to `main`.

---

## 7. Write-Audit-Publish (WAP) Pattern Orchestration

To prevent unverified transactions and half-baked batch runs from polluting the canonical `main` branch, IceFlow Core features an automated **Write-Audit-Publish (WAP)** orchestrator (`catalog/wap_orchestrator.py`):

```bash
make wap-pipeline
```

```text
1. [WRITE]  Stage writes on isolated branch: `wap/stage-20261021-100800`
2. [AUDIT]  Execute full verification gauntlet:
            - dbt Staging Contract Enforcement
            - Singular SQL Domain Assertions (Negative spreads, boundaries)
            - Soda Core SLA Freshness, Schema, and Volume Variance Gates
            - Open Data Contract Gatekeeper (ODCS)
3. [PUBLISH] If all gates pass: Atomically fast-forward merge staging branch into `main`.
             If any gate breaches: Abort merge, isolate staging branch, alert CI.
```

---

## 8. Ingestion Quarantine & Dead-Letter Queue (DLQ)

Upstream scrapers run strict Pydantic contract validation at the Bronze edge before disk serialization. Corrupted payloads (negative prices, invalid ISO currency codes, inverted margins) are intercepted:
- **Clean data:** Persisted to Bronze Parquet (`s3://bronze/market_commodities/...`).
- **Malformed data:** Diverted to the **Quarantine Dead-Letter Queue** (`s3://bronze/quarantine/<dataset>/quarantine.jsonl`) with ISO timestamp, root violation reasons, and raw payload retention.

```bash
# List all quarantined telemetry records
make quarantine-list

# Inspect deep diagnostic details for a specific quarantined item
make quarantine-inspect ID=<quarantine_id>
```

---

## 9. Chaos CI Negative-Test Suite

To mathematically prove that our defensive barriers actually halt corrupted payloads (rather than giving false positives), the platform provides an automated **Chaos CI Negative-Test Suite** (`tests/chaos_test_suite.py`):

```bash
make test-chaos
```

The suite injects controlled defects across 7 isolated lakehouse dimensions:
1. **CHAOS-01 (Quarantine Trap):** Injects negative prices and corrupted currencies into `cbsl_scraper.py`; asserts segregation into DLQ.
2. **CHAOS-02 (Schema Drift Trap):** Drops required columns and mutates numeric types; asserts ODCS Gatekeeper exits with code 1.
3. **CHAOS-03 (Negative Spread Trap):** Injects retail < wholesale price inversions; asserts dbt singular test trips.
4. **CHAOS-04 (Physical Boundary Trap):** Injects non-positive prices; asserts boundary assertion catches violations.
5. **CHAOS-05 (Volatility Spike Trap):** Injects +158% day-over-day price spike; asserts volatility anomaly gate trips.
6. **CHAOS-06 (Surrogate Key Trap):** Injects duplicate and NULL surrogate keys; asserts uniqueness/nullity gate blocks merge.
7. **CHAOS-07 (WAP Abort Trap):** Simulates audit failure on staging branch; asserts fast-forward merge is blocked and `main` is preserved.

> **Verification Rule:** A chaos test passes **only when the platform gate successfully catches and blocks the defect**.
