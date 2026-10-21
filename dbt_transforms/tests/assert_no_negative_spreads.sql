-- Custom Singular dbt Domain Test: assert_no_negative_spreads.sql
-- ==============================================================================
-- Domain Rule: In healthy commodity distribution markets, retail prices must
-- always meet or exceed wholesale acquisition prices (spread >= 0).
-- Negative spreads indicate corrupted price ticks, inverted units, or fraudulent telemetry.
--
-- Pass Criteria: 0 rows returned
-- Fail Criteria: Any row returned represents an invalid negative spread breach.
-- ==============================================================================

select
    commodity_sk,
    commodity_code,
    commodity_name,
    market_location,
    recorded_date,
    wholesale_price_lkr,
    retail_price_lkr,
    (retail_price_lkr - wholesale_price_lkr) as negative_spread_lkr,
    round(((retail_price_lkr - wholesale_price_lkr) / wholesale_price_lkr) * 100.0, 2) as margin_pct
from {{ ref('stg_cbsl_commodity_prices') }}
where retail_price_lkr < wholesale_price_lkr
