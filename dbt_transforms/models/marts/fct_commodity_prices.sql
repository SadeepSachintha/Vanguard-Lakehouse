with stg_commodities as (
    select * from {{ ref('stg_market_commodities') }}
),

dim_comm as (
    select commodity_key, commodity_id from {{ ref('dim_commodities') }}
),

dim_prov as (
    select province_key, market_center from {{ ref('dim_provinces') }}
),

dim_d as (
    select date_key, calendar_date from {{ ref('dim_dates') }}
),

joined as (
    select
        c.commodity_price_surrogate_key as fact_price_key,

        -- Foreign Keys to Star Schema Dimensions
        dc.commodity_key,
        dd.date_key,
        dp.province_key,

        -- Natural Business Keys & Degenerate Dimensions
        c.commodity_id,
        c.market_center,
        c.trade_date,
        c.quality_grade,
        c.unit_of_measure,

        -- Additive Financial Measures
        c.wholesale_price_lkr,
        c.retail_price_lkr,
        cast((c.retail_price_lkr - c.wholesale_price_lkr) as decimal(18, 2)) as retail_wholesale_spread_lkr,
        cast(
            round(((c.retail_price_lkr - c.wholesale_price_lkr) / c.wholesale_price_lkr) * 100.0, 2)
            as decimal(18, 2)
        ) as retail_markup_pct,

        -- Analytical Window: Previous Day Wholesale Price
        lag(c.wholesale_price_lkr) over (
            partition by c.commodity_id, c.market_center
            order by c.trade_date
        ) as prev_day_wholesale_price_lkr,

        c.ingested_at

    from stg_commodities c
    inner join dim_comm dc on c.commodity_id = dc.commodity_id
    inner join dim_prov dp on c.market_center = dp.market_center
    inner join dim_d dd on c.trade_date = dd.calendar_date
)

select
    fact_price_key,
    commodity_key,
    date_key,
    province_key,
    commodity_id,
    market_center,
    trade_date,
    quality_grade,
    unit_of_measure,
    wholesale_price_lkr,
    retail_price_lkr,
    retail_wholesale_spread_lkr,
    retail_markup_pct,
    prev_day_wholesale_price_lkr,

    -- Day-over-Day Price Delta & Quality Gate Anomaly Detection
    case
        when prev_day_wholesale_price_lkr is not null and prev_day_wholesale_price_lkr > 0
        then cast(
            round(((wholesale_price_lkr - prev_day_wholesale_price_lkr) / prev_day_wholesale_price_lkr) * 100.0, 2)
            as decimal(18, 2)
        )
        else 0.0
    end as day_over_day_price_change_pct,

    case
        when prev_day_wholesale_price_lkr is not null and prev_day_wholesale_price_lkr > 0
             and abs(((wholesale_price_lkr - prev_day_wholesale_price_lkr) / prev_day_wholesale_price_lkr)) > 0.50
        then true
        else false
    end as is_price_spike_anomaly,

    ingested_at

from joined
