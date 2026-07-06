{{ config(
    materialized = 'incremental',
    unique_key = 'date_key'
) }}

select
    dd.date_key,

    w.temperature_max,
    w.temperature_min,
    w.temperature_high,
    w.temperature_low,
    w.humidity,
    w.pressure,
    w.wind_speed,
    w.wind_bearing,
    w.cloud_cover,
    w.visibility,
    w.dew_point,
    w.uv_index,
    w.sunrise_time,
    w.sunset_time,
    w.moon_phase,
    w.precip_type,
    w.weather_summary,

    current_timestamp() as gold_updated_at

from {{ source('silver', 'weather_daily') }} w

inner join {{ ref('dim_date') }} dd
    on cast(w.weather_date as date) = dd.full_date