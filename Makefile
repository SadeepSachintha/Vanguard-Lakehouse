.PHONY: help up down init-buckets ingest iceberg-write dbt-run dbt-test dbt-snapshot contract-gate test-contracts test-chaos soda-scan time-travel-audit olap pipeline wap-pipeline quarantine-list quarantine-inspect clean

HAS_VENV := $(shell test -f "$(CURDIR)/.venv/bin/python" && echo yes)
ifeq ($(HAS_VENV),yes)
    PYTHON ?= "$(CURDIR)/.venv/bin/python"
    DBT ?= "$(CURDIR)/.venv/bin/dbt"
else
    PYTHON ?= python3
    DBT ?= dbt
endif

help:
	@echo "=========================================================================="
	@echo "  Vanguard-Lakehouse: Data Contracts, Quality & Gatekeeping Pipeline"
	@echo "=========================================================================="
	@echo "  make wap-pipeline      - Execute automated Write-Audit-Publish (WAP) cycle"
	@echo "  make test-contracts    - Run local developer CI quality gate CLI harness"
	@echo "  make test-chaos        - Execute Chaos CI negative-test defect injection suite"
	@echo "  make soda-scan         - Execute Soda Core schema, SLA, and anomaly checks"
	@echo "  make quarantine-list   - List all quarantined records in the Bronze DLQ"
	@echo "  make quarantine-inspect - Inspect specific quarantined record (ID=<quarantine_id>)"
	@echo "  make up                - Start MinIO and Nessie Catalog containers"
	@echo "  make down              - Stop all running lakehouse containers"
	@echo "  make init-buckets      - Provision MinIO bronze, silver, gold, warehouse buckets"
	@echo "  make ingest            - Scrape CBSL Forex & market commodities (Bronze)"
	@echo "  make iceberg-write     - Convert Bronze to Iceberg tables with hidden partitions & sort clustering"
	@echo "  make iceberg-benchmark - Run Iceberg row-group min-max pruning benchmark"
	@echo "  make dbt-run           - Execute dbt transformations (Silver & Gold star schema)"
	@echo "  make dbt-test          - Run dbt quality assertions & referential integrity tests"
	@echo "  make dbt-snapshot      - Run dbt SCD Type 2 history snapshots"
	@echo "  make contract-gate     - Execute Automated Contract Gateway & Anomaly Checks"
	@echo "  make time-travel-audit - Execute point-in-time Iceberg audit query in DuckDB"
	@echo "  make olap              - Run sub-second OLAP query on Gold commodity prices"
	@echo "  make pipeline          - Run full end-to-end lakehouse pipeline"
	@echo "  make clean             - Clean local database and temporary build artifacts"
	@echo "=========================================================================="

up:
	docker-compose up -d minio nessie minio-init-buckets
	@echo "MinIO (S3) available at: http://localhost:9000 (Console: http://localhost:9001)"
	@echo "Nessie Catalog available at: http://localhost:19120"

down:
	docker-compose down

init-buckets:
	docker-compose up minio-init-buckets

ingest:
	$(PYTHON) ingestion/cbsl_scraper.py --start-date 2026-08-01 --end-date 2026-10-21 --no-s3

iceberg-write:
	$(PYTHON) ingestion/write_to_iceberg.py --branch feature/q3-reconcile --maintenance --verify-pruning

iceberg-benchmark:
	$(PYTHON) ingestion/write_to_iceberg.py --verify-pruning

dbt-run:
	cd dbt_transforms && $(DBT) run --profiles-dir .

dbt-test:
	cd dbt_transforms && $(DBT) test --profiles-dir .

dbt-snapshot:
	cd dbt_transforms && $(DBT) snapshot --profiles-dir .

contract-gate:
	$(PYTHON) contracts/contract_gateways.py \
		--db dbt_transforms/iceflow_lakehouse.duckdb \
		--contracts-yml dbt_transforms/models/marts/_marts_models.yml \
		--threshold 50.0

test-contracts:
	bash scripts/run_quality_gate.sh

test-chaos:
	$(PYTHON) tests/chaos_test_suite.py

soda-scan:
	$(PYTHON) soda/soda_runner.py \
		--db dbt_transforms/iceflow_lakehouse.duckdb \
		--checks soda/checks_commodities.yml \
		--config soda/configuration.yml

time-travel-audit:
	$(PYTHON) analytics/query_time_travel_duckdb.py \
		--db dbt_transforms/iceflow_lakehouse.duckdb \
		--commodity RICE_SAMBA \
		--as-of "2026-10-01 00:00:00 UTC"

olap:
	$(PYTHON) analytics/query_time_travel_duckdb.py \
		--db dbt_transforms/iceflow_lakehouse.duckdb \
		--commodity RICE_SAMBA \
		--olap-only

pipeline: ingest iceberg-write dbt-run dbt-test dbt-snapshot contract-gate time-travel-audit
	@echo "🎉 End-to-end IceFlow Core Lakehouse pipeline completed successfully!"

wap-pipeline:
	$(PYTHON) catalog/wap_orchestrator.py --base main

quarantine-list:
	$(PYTHON) ingestion/quarantine_inspector.py list

quarantine-inspect:
	@if [ -z "$(ID)" ]; then \
		echo "Usage: make quarantine-inspect ID=<quarantine_record_id>"; \
		exit 1; \
	fi
	$(PYTHON) ingestion/quarantine_inspector.py inspect $(ID)

clean:
	rm -rf bronze_data iceberg_warehouse dbt_transforms/iceflow_lakehouse.duckdb* dbt_transforms/target dbt_transforms/dbt_packages
	@echo "Cleaned lakehouse warehouse and build targets."
