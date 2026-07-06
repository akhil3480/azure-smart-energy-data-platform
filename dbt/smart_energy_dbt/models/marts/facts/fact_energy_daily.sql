{{ config(
    materialized = 'incremental',
    unique_key = ['household_key', 'date_key']
) }}

select

    dh.household_key,
    dd.date_key,

    e.energy_median,
    e.energy_mean,
    e.energy_max,
    e.energy_count,
    e.energy_std,
    e.energy_sum,
    e.energy_min,

    e.data_quality_status,

    current_timestamp() as gold_updated_at

from {{ source('silver', 'energy_daily') }} e

inner join {{ ref('dim_household') }} dh
    on e.lclid = dh.lclid

inner join {{ ref('dim_date') }} dd
    on cast(e.day as date) = dd.full_date