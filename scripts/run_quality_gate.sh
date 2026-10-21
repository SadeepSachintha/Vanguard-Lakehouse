#!/usr/bin/env bash
# ==============================================================================
# Vanguard-Lakehouse: Local Developer Quality Gate CLI Harness
# Executes the exact CI data contract, domain integrity, and anomaly assertions.
# ==============================================================================

set -o errexit
set -o pipefail
set -o nounset

# ANSI Color Codes for Rich Terminal Output
BOLD="\033[1m"
GREEN="\033[38;5;82m"
RED="\033[38;5;196m"
YELLOW="\033[38;5;220m"
BLUE="\033[38;5;39m"
PURPLE="\033[38;5;141m"
RESET="\033[0m"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo -e "${BOLD}${PURPLE}"
echo "=============================================================================="
echo "    🛡️  VANGUARD LAKEHOUSE LOCAL DATA QUALITY GATE CLI HARNESS               "
echo "=============================================================================="
echo -e "${RESET}"

# Auto-detect python / dbt environment
if [ -f "${REPO_ROOT}/.venv/bin/python" ]; then
    PYTHON="${REPO_ROOT}/.venv/bin/python"
    DBT="${REPO_ROOT}/.venv/bin/dbt"
    echo -e "${BLUE}ℹ️  Using virtual environment: ${REPO_ROOT}/.venv${RESET}"
else
    PYTHON="python3"
    DBT="dbt"
    echo -e "${YELLOW}ℹ️  Virtual environment not found, defaulting to system python3 & dbt${RESET}"
fi

STEP_FAILED=0

# Step 1: Pre-requisite Ingestion Check
echo -e "\n${BOLD}${BLUE}==> [1/5] Ingesting Bronze CBSL Telemetry & Iceberg Conversion...${RESET}"
if "${PYTHON}" ingestion/cbsl_scraper.py --start-date 2026-08-01 --end-date 2026-10-21 --no-s3 && \
   "${PYTHON}" ingestion/write_to_iceberg.py --branch feature/local-dev-gate --maintenance; then
    echo -e "${GREEN}✅ [1/5] Bronze ingestion & Iceberg hidden partitioning passed.${RESET}"
else
    echo -e "${RED}❌ [1/5] Bronze ingestion failed!${RESET}"
    exit 1
fi

# Step 2: dbt Compile
echo -e "\n${BOLD}${BLUE}==> [2/5] Compiling dbt Models & Enforcing Project Syntax...${RESET}"
if (cd "${REPO_ROOT}/dbt_transforms" && "${DBT}" compile --profiles-dir .); then
    echo -e "${GREEN}✅ [2/5] dbt compilation successful.${RESET}"
else
    echo -e "${RED}❌ [2/5] dbt compilation failed! Check SQL syntax and macros.${RESET}"
    exit 1
fi

# Step 3: dbt Staging Enforced Contracts
echo -e "\n${BOLD}${BLUE}==> [3/5] Enforcing Machine-Readable Model Contracts on Silver Tier...${RESET}"
if (cd "${REPO_ROOT}/dbt_transforms" && "${DBT}" run --select tag:staging --profiles-dir .); then
    echo -e "${GREEN}✅ [3/5] Enforced model contracts verified on staging tier.${RESET}"
else
    echo -e "${RED}❌ [3/5] Model contract breached! Schema drift or type mutation detected.${RESET}"
    exit 1
fi

# Step 4: Run All dbt Models & Tests (including assert_no_negative_spreads)
echo -e "\n${BOLD}${BLUE}==> [4/5] Running dbt Marts & Domain Integrity Tests...${RESET}"
(cd "${REPO_ROOT}/dbt_transforms" && "${DBT}" run --profiles-dir .)
if (cd "${REPO_ROOT}/dbt_transforms" && "${DBT}" test --profiles-dir .); then
    echo -e "${GREEN}✅ [4/5] All singular domain tests and referential integrity assertions passed.${RESET}"
else
    echo -e "${RED}❌ [4/5] dbt test assertions failed! Review offending rows above.${RESET}"
    exit 1
fi

# Step 5: Soda Core Quality & Anomaly Suite
echo -e "\n${BOLD}${BLUE}==> [5/5] Executing Soda Core Statistical Distribution & Anomaly Suite...${RESET}"
if "${PYTHON}" soda/soda_runner.py \
    --db "${REPO_ROOT}/dbt_transforms/iceflow_lakehouse.duckdb" \
    --checks "${REPO_ROOT}/soda/checks_commodities.yml" \
    --config "${REPO_ROOT}/soda/configuration.yml"; then
    echo -e "${GREEN}✅ [5/5] Soda Core schema validation, SLAs, and anomaly checks passed.${RESET}"
else
    echo -e "${RED}❌ [5/5] Soda Core quality gate failed! Review failures above.${RESET}"
    exit 1
fi

echo -e "\n${BOLD}${GREEN}"
echo "=============================================================================="
echo "    🎉 ALL QUALITY GATES PASSED! MERGE APPROVED FOR VANGUARD-LAKEHOUSE        "
echo "=============================================================================="
echo -e "${RESET}"

exit 0
