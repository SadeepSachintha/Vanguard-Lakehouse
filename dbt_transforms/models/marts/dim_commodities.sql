with staging_commodities as (
    select * from {{ ref('stg_market_commodities') }}
)

select
    -- Surrogate Primary Key
    {{ generate_surrogate_key(['commodity_id']) }} as commodity_key,

    -- Business Natural Key
    commodity_id,

    -- Dimensional Attributes
    commodity_name,
    commodity_category,
    max(unit_of_measure) as default_unit_of_measure,
    min(trade_date) as first_recorded_date,
    max(trade_date) as last_recorded_date,
    count(distinct trade_date) as total_reporting_days

from staging_commodities
group by commodity_id, commodity_name, commodity_category
