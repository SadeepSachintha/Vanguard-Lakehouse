with raw_source as (
    select * from {{ read_bronze_source('cbsl_forex') }}
),

standardized as (
    select
        -- Deterministic SHA-256 Surrogate Key
        {{ generate_surrogate_key(['currency_code', 'trade_date']) }} as forex_tick_surrogate_key,

        -- Standardized currency identifiers
        upper(trim(currency_code)) as currency_code,
        upper(trim(base_currency)) as base_currency,

        -- Explicit financial decimals (4 decimal places for currency exchange)
        cast(indicative_rate as decimal(18, 4)) as indicative_rate,
        cast(buying_rate as decimal(18, 4)) as buying_rate,
        cast(selling_rate as decimal(18, 4)) as selling_rate,

        -- Spread calculation in LKR
        cast((selling_rate - buying_rate) as decimal(18, 4)) as bid_ask_spread_lkr,

        -- ISO date and timestamp casting
        cast(trade_date as date) as trade_date,
        cast(extracted_at as timestamp) as ingested_at

    from raw_source
    where indicative_rate > 0
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by forex_tick_surrogate_key
            order by ingested_at desc
        ) as deduplication_rank
    from standardized
)

select
    forex_tick_surrogate_key,
    currency_code,
    base_currency,
    indicative_rate,
    buying_rate,
    selling_rate,
    bid_ask_spread_lkr,
    trade_date,
    ingested_at
from deduplicated
where deduplication_rank = 1
