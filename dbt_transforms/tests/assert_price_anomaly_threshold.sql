-- IceFlow Core Anomaly Gating Assertion
-- Flags unexpected day-over-day price volatility exceeding 50%
-- In dbt, returning any rows indicates a test failure.

select
    fact_price_key,
    commodity_id,
    market_center,
    trade_date,
    prev_day_wholesale_price_lkr,
    wholesale_price_lkr,
    day_over_day_price_change_pct
from {{ ref('fct_commodity_prices') }}
where abs(day_over_day_price_change_pct) > 50.0
