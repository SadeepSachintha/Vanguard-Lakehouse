with date_spine as (
    select
        cast(range as date) as calendar_date
    from range(date '2026-01-01', date '2027-12-31', interval 1 day)
)

select
    -- Integer Date Key (YYYYMMDD) for fast star-schema joins
    cast(strftime(calendar_date, '%Y%m%d') as integer) as date_key,

    -- Calendar Attributes
    calendar_date,
    extract('year' from calendar_date) as year_number,
    extract('quarter' from calendar_date) as quarter_number,
    extract('month' from calendar_date) as month_number,
    strftime(calendar_date, '%B') as month_name,
    strftime(calendar_date, '%Y-%m') as year_month,
    extract('day' from calendar_date) as day_of_month,
    extract('dayofweek' from calendar_date) as day_of_week,
    strftime(calendar_date, '%A') as day_name,

    -- Flags & Indicators
    case when extract('dayofweek' from calendar_date) in (0, 6) then true else false end as is_weekend,
    'FY' || cast(extract('year' from calendar_date) as varchar) as fiscal_year

from date_spine
order by calendar_date
