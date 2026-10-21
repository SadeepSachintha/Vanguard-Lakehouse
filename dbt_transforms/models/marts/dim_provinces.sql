with staging_centers as (
    select distinct
        market_center,
        province_name
    from {{ ref('stg_market_commodities') }}
)

select
    -- Surrogate Primary Key
    {{ generate_surrogate_key(['market_center']) }} as province_key,

    -- Dimensional Attributes
    market_center,
    province_name,

    case
        when market_center like '%Pettah%' then 'National Commercial Wholesale Hub'
        when market_center like '%Dambulla%' then 'Apex National Agricultural Collection Hub'
        when market_center like '%Narahenpita%' then 'Metropolitan Retail & Consumer Center'
        when market_center like '%Kandy%' then 'Central Highlands Distribution Center'
        when market_center like '%Thambuththegama%' then 'Dry Zone Paddy & Produce Center'
        else 'Regional Trading Hub'
    end as market_hub_role,

    case
        when province_name in ('Western', 'Southern', 'Sabaragamuwa') then 'Wet Zone'
        when province_name in ('Central', 'Uva') then 'Hill Country / Intermediate Zone'
        when province_name in ('North Central', 'Northern', 'Eastern', 'North Western') then 'Dry Zone'
        else 'Standard Agro-Climatic Zone'
    end as agro_climatic_zone

from staging_centers
