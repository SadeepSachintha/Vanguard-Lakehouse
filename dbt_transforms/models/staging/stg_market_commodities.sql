with raw_source as (
    select * from {{ read_bronze_source('market_commodities') }}
),

standardized as (
    select
        -- SHA-256 Surrogate Key for Row Deduplication
        {{ generate_surrogate_key(['commodity_id', 'market_center', 'trade_date']) }} as commodity_price_surrogate_key,

        -- Standardized identifiers and descriptive attributes
        upper(trim(commodity_id)) as commodity_id,
        trim(commodity_name) as commodity_name,
        trim(category) as commodity_category,
        trim(market_center) as market_center,
        trim(province) as province_name,
        coalesce(nullif(trim(grade), ''), 'Standard Grade') as quality_grade,
        coalesce(nullif(trim(unit), ''), 'Rs/kg') as unit_of_measure,

        -- Explicit Financial Decimal Casts (prevents float precision errors)
        cast(wholesale_price as decimal(18, 2)) as wholesale_price_lkr,
        cast(retail_price as decimal(18, 2)) as retail_price_lkr,

        -- Temporal attributes parsed as ISO dates and timestamps
        cast(trade_date as date) as trade_date,
        cast(extracted_at as timestamp) as ingested_at

    from raw_source
    where wholesale_price > 0 and retail_price > 0
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by commodity_price_surrogate_key
            order by ingested_at desc
        ) as deduplication_rank
    from standardized
)

select
    commodity_price_surrogate_key,
    commodity_id,
    commodity_name,
    commodity_category,
    market_center,
    province_name,
    quality_grade,
    unit_of_measure,
    wholesale_price_lkr,
    retail_price_lkr,
    trade_date,
    ingested_at
from deduplicated
where deduplication_rank = 1
