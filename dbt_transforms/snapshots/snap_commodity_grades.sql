{% snapshot snap_commodity_grades %}

{#
    IceFlow Core SCD Type 2 Snapshot
    Tracks historical metadata changes in commodity item grade classifications
    and regional reporting market hubs over time.
    Maintains dbt_valid_from and dbt_valid_to columns for historical point-in-time audits.
#}

{{
    config(
        target_schema='gold',
        unique_key='commodity_grade_key',
        strategy='check',
        check_cols=['quality_grade', 'market_center'],
        invalidate_hard_deletes=True
    )
}}

with source_data as (
    select
        -- Unique surrogate key per commodity and market hub combination
        {{ generate_surrogate_key(['commodity_id', 'market_center']) }} as commodity_grade_key,
        commodity_id,
        commodity_name,
        market_center,
        province_name,
        quality_grade,
        trade_date as effective_date,
        ingested_at
    from {{ ref('stg_market_commodities') }}
)

select
    commodity_grade_key,
    commodity_id,
    commodity_name,
    market_center,
    province_name,
    quality_grade,
    effective_date,
    ingested_at
from source_data

{% endsnapshot %}
