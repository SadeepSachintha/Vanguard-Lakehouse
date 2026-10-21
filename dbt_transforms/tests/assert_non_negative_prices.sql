-- IceFlow Core Quality Assertion
-- Tests that wholesale and retail prices are strictly positive and retail exceeds wholesale.
-- In dbt, returning any rows indicates a test failure.

select
    fact_price_key,
    commodity_id,
    market_center,
    trade_date,
    wholesale_price_lkr,
    retail_price_lkr
from {{ ref('fct_commodity_prices') }}
where wholesale_price_lkr <= 0
   or retail_price_lkr <= 0
