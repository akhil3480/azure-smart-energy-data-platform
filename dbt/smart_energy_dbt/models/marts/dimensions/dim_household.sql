{{ config(
    materialized = 'incremental',
    unique_key = 'lclid'
) }}

select
    abs(xxhash64(lclid)) as household_key,
    lclid,
    tariff_type,
    acorn,
    acorn_grouped,
    current_timestamp() as gold_updated_at

from {{ source('silver', 'household') }}

where lclid is not null