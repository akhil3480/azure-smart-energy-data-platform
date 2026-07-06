{{ config(
    materialized = 'table'
) }}

with all_dates as (

    select cast(day as date) as full_date
    from {{ source('silver', 'energy_daily') }}
    where day is not null

    union

    select cast(weather_date as date) as full_date
    from {{ source('silver', 'weather_daily') }}
    where weather_date is not null

    union

    select cast(holiday_date as date) as full_date
    from {{ source('silver', 'holiday') }}
    where holiday_date is not null
),

holiday_data as (

    select
        cast(holiday_date as date) as holiday_date,
        holiday_name
    from {{ source('silver', 'holiday') }}

),

final as (

    select
        cast(date_format(d.full_date, 'yyyyMMdd') as int) as date_key,
        d.full_date,

        year(d.full_date) as year,
        quarter(d.full_date) as quarter,
        month(d.full_date) as month,
        date_format(d.full_date, 'MMMM') as month_name,
        weekofyear(d.full_date) as week_of_year,
        day(d.full_date) as day_of_month,
        dayofweek(d.full_date) as day_of_week,
        date_format(d.full_date, 'EEEE') as day_name,

        case
            when dayofweek(d.full_date) in (1, 7) then true
            else false
        end as is_weekend,

        case
            when month(d.full_date) in (12, 1, 2) then 'Winter'
            when month(d.full_date) in (3, 4, 5) then 'Spring'
            when month(d.full_date) in (6, 7, 8) then 'Summer'
            when month(d.full_date) in (9, 10, 11) then 'Autumn'
        end as season,

        case
            when h.holiday_date is not null then true
            else false
        end as is_bank_holiday,

        h.holiday_name,

        current_timestamp() as gold_updated_at

    from all_dates d
    left join holiday_data h
        on d.full_date = h.holiday_date
)

select *
from final