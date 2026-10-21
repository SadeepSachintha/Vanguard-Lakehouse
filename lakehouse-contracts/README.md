# Vanguard Lakehouse: Data Contract & Testing Gatekeeping Architecture

## 1. Executive Summary & Objective

The **Vanguard Lakehouse** contract gatekeeping framework guarantees that upstream open data telemetry (such as Central Bank of Sri Lanka daily market commodity prices and foreign exchange rates) conforms to strict contractual SLAs before landing in conformed analytical tiers.

By enforcing **machine-readable Data Contracts** at the dbt model boundary and coupling them with **Soda Core statistical distribution tests** in GitHub Actions CI, we prevent:
1. **Upstream Schema Drift:** Scrapers adding unexpected columns or mutating data types are rejected at compile/run time.
2. **Silent Data Corruption:** Negative spreads (where consumer retail prices drop below wholesale acquisition costs) are halted immediately by custom singular SQL assertions.
3. **Statistical Outliers:** Sudden day-over-day price spikes ($\Delta > 50\%$) and volume drops (>20% from a rolling 7-day baseline) trigger gatekeeper halts.

---

## 2. Medallion Data Flow & Gatekeeping Checkpoints

```
 ┌────────────────────────────────────────────────────────┐
 │           External Data Ingestors (Airflow)            │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼  [Checkpoint 1: Pydantic Schema Validation]
 ┌────────────────────────────────────────────────────────┐
 │        Bronze Tier: Raw Parquet/JSON in MinIO          │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼  [Checkpoint 2: Enforced dbt Model Contract]
 ┌────────────────────────────────────────────────────────┐
 │   Silver Tier: stg_cbsl_commodity_prices (Iceberg)     │
 │   - Enforced schema typing (DECIMAL(10,2), DATE)       │
 │   - SHA-256 surrogate row deduplication                │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼  [Checkpoint 3: Singular SQL Domain Assertions]
 ┌────────────────────────────────────────────────────────┐
 │   assert_no_negative_spreads (retail >= wholesale)     │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼  [Checkpoint 4: Soda Core Anomaly Suite]
 ┌────────────────────────────────────────────────────────┐
 │   - 24h Freshness SLA                                  │
 │   - Rolling 7-day volume baseline (>20% drop alert)    │
 │   - Price movement volatility guard (|Δ| > 50%)        │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼  [Checkpoint 5: CI/CD Merge Gate]
 ┌────────────────────────────────────────────────────────┐
 │   Gold Tier: fct_commodity_prices, dim_commodities     │
 │   - Star Schema Marts & SCD Type 2 Snapshots           │
 └────────────────────────────────────────────────────────┘
```

---

## 3. Specifications Summary

| Component | Path | Responsibility |
| :--- | :--- | :--- |
| **dbt Model Contract** | `dbt_transforms/models/staging/stg_cbsl_commodity_prices.yml` | Strict compilation barrier (`contract: { enforced: true }`) |
| **Soda Core Suite** | `soda/checks_commodities.yml` | SLA validation, volume continuity, price anomalies |
| **Singular Test** | `dbt_transforms/tests/assert_no_negative_spreads.sql` | Business domain assertion (`retail >= wholesale`) |
| **ODCS Schema** | `lakehouse-contracts/schemas/stg_cbsl_commodity_prices.contract.json` | Open Data Contract Standard machine-readable definition |
| **CI Workflow** | `.github/workflows/data_quality_ci.yml` | PR merge-blocking gate in GitHub Actions |
| **Local CLI** | `scripts/run_quality_gate.sh` | Local developer test harness (`make test-contracts`) |

---

## 4. Local Execution

```bash
# Run the complete test suite locally
make test-contracts

# Or execute specific checks
make dbt-run           # Enforces dbt model contracts
make dbt-test          # Runs all singular and generic SQL assertions
make soda-scan         # Executes Soda Core statistical gatekeeper
```
