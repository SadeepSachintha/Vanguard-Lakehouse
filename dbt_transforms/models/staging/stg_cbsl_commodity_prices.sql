{{
    config(
        materialized='table',
        tags=['staging', 'cbsl', 'commodities']
    )
}}

with raw_source as (
    select * from {{ read_bronze_source('market_commodities') }}
),

standardized as (
    select
        -- SHA-256 Deterministic Surrogate Primary Key for Row Deduplication & Contract Enforcement
        {{ generate_surrogate_key(['commodity_id', 'market_center', 'trade_date']) }} as commodity_sk,

        -- Temporal Dimensions
        cast(trade_date as date) as recorded_date,

        -- Standardized Identifiers & Locations
        upper(trim(commodity_id)) as commodity_code,
        trim(commodity_name) as commodity_name,
        trim(market_center) as market_location,

        -- Strict Financial Decimal Typing (Prevents Floating-Point Drift)
        cast(retail_price as decimal(10, 2)) as retail_price_lkr,
        cast(wholesale_price as decimal(10, 2)) as wholesale_price_lkr,

        -- Telemetry Lineage Timestamp
        cast(extracted_at as timestamp) as ingested_at

    from raw_source
    where wholesale_price > 0 and retail_price > 0
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by commodity_sk
            order by ingested_at desc
        ) as deduplication_rank
    from standardized
)

select
    commodity_sk,
    recorded_date,
    commodity_code,
    commodity_name,
    market_location,
    retail_price_lkr,
    wholesale_price_lkr,
    ingested_at
from deduplicated
where deduplication_rank = 1
