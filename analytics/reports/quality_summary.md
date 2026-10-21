# 🛡️ Vanguard Lakehouse Executive SLA & Quality Report

**Audit Timestamp:** `2026-10-21 05:09:04 UTC` | **Platform Health:** `100% HEALTHY`

### 📊 Key Performance & Compliance Indicators
| Metric | Value | Status | Target SLA |
| :--- | :--- | :--- | :--- |
| **Data Contract Compliance** | **`100.0%`** | 🟢 PASS | 100% Enforced |
| **Clean Fact Observations** | **`4,920` rows** | 🟢 STABLE | Continuous Flow |
| **Iceberg Row-Group Pruning** | **`90.9%` skipped** | ⚡ SUB-SECOND | &ge; 75% Pruned |
| **DuckDB OLAP Latency** | **`1.78 ms`** | ⚡ HIGH PERF | < 100 ms |
| **Quarantine DLQ Count** | **`2` isolated** | 🛡️ DEFENDED | 0 Leaked to Silver |
| **Chaos Defenses Trapped** | **`7 / 7` blocked** | 🔒 PROVEN | 100% Interception |

---

### 📑 Machine-Readable Data Contracts (ODCS)
| Tier | Model | Primary Key | Rows | Columns | Constraints | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `Gold Fact` | `fct_commodity_prices` | `fact_price_key` | `4,920` | `17` | `0 NULLs / 0 DUPs` | 🟢 **PASS** |
| `Gold Dim` | `dim_commodities` | `commodity_key` | `12` | `8` | `0 NULLs / 0 DUPs` | 🟢 **PASS** |
| `Gold Dim` | `dim_dates` | `date_key` | `729` | `12` | `0 NULLs / 0 DUPs` | 🟢 **PASS** |
| `Gold Dim` | `dim_provinces` | `province_key` | `5` | `5` | `0 NULLs / 0 DUPs` | 🟢 **PASS** |
| `Silver Staging` | `stg_cbsl_commodity_prices` | `commodity_sk` | `4,920` | `8` | `0 NULLs / 0 DUPs` | 🟢 **PASS** |

---

### 🔍 Quality Assertions & Soda Core Checks
| Suite | Check Name | Target Relation | Condition | Observed Value | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `Soda Core` | **Freshness SLA (< 24h Telemetry Gap)** | `fct_commodity_prices.trade_date` | `MAX(trade_date) >= CURRENT_DATE - 1` | Latest: 2026-10-21 | 🟢 **PASS** |
| `dbt Singular` | **Negative Price Spread Prohibition** | `stg_cbsl_commodity_prices` | `retail_price >= wholesale_price (mustBe: 0)` | 0 inverted records | 🟢 **PASS** |
| `dbt Singular` | **Strict Positive Price Bounds** | `fct_commodity_prices.prices` | `wholesale > 0 AND retail > 0 (mustBe: 0)` | 0 violations | 🟢 **PASS** |
| `Soda Core` | **Day-over-Day Price Volatility Anomaly** | `fct_commodity_prices.volatility` | `|day_over_day_price_change_pct| <= 50.0%` | 0 anomalies (Max Delta: 0.00%) | 🟢 **PASS** |
| `dbt Core` | **Foreign Key Integrity: dim_commodities** | `fct_commodity_prices -> dim_commodities` | `Zero orphan foreign keys` | 0 orphan records | 🟢 **PASS** |
| `Soda Core` | **7-Day Volume Drop Stability (<20% variance)** | `Bronze Ingestion Streams` | `Variance <= 20%` | Zero unexpected volume drops | 🟢 **PASS** |

---

### 🧪 Chaos CI Defensive Negative-Test Suite
| ID | Chaos Scenario | Target Defense Gate | Injected Defect | Action |
| :--- | :--- | :--- | :--- | :--- |
| `CHAOS-01` | **Ingestion Quarantine DLQ Trap** | Pydantic Ingestion Gatekeeper | `Negative wholesale price (-50 LKR) & invalid currency (USD_FAKE)` | ✅ BLOCKED (PASSED) |
| `CHAOS-02` | **Schema Contract Mutation & Column Drift** | ODCS Gatekeeper | `Dropped required column 'retail_price_lkr' & mutated wholesale to VARCHAR` | ✅ BLOCKED (PASSED) |
| `CHAOS-03` | **Negative Price Spread Domain Breach** | Singular dbt Assertion | `Inverted retail-wholesale price spread (Spread = -170.00 LKR)` | ✅ BLOCKED (PASSED) |
| `CHAOS-04` | **Physical Boundary Rule (Non-Positive Prices)** | Singular dbt Assertion | `Wholesale price <= 0 and retail price = 0.00 LKR` | ✅ BLOCKED (PASSED) |
| `CHAOS-05` | **Day-over-Day Price Volatility Anomaly** | Soda Core Volatility Gatekeeper | `Day-over-day price jump +158.33% (threshold: 50.0%)` | ✅ BLOCKED (PASSED) |
| `CHAOS-06` | **Primary Key Duplicate & Nullity Breach** | ODCS Gatekeeper | `Injected duplicate surrogate keys ('sk-DUP') and a NULL surrogate key` | ✅ BLOCKED (PASSED) |
| `CHAOS-07` | **WAP Fast-Forward Merge Abort on Failure** | Write-Audit-Publish Orchestrator | `Staged breaking schema on staging branch; asserted main preserved` | ✅ BLOCKED (PASSED) |

---

### ⚡ Iceberg Row-Group Pruning Benchmark
- **Target Table:** `bronze.market_commodities`
- **Filter:** `commodity_id = 'RICE_SAMBA'`
- **Total Row Groups:** `198`
- **Row Groups Skipped (Pruned):** `180`
- **Pruning Efficiency:** **`90.9%` OF ROW GROUPS PRUNED**
- **DuckDB Query Latency:** **`1.78 ms`**

> **Platform Guarantee:** Sub-second OLAP queries with zero expensive cloud data warehouse compute fees.
