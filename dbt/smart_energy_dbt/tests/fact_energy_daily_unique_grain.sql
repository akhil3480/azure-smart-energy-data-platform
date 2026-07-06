select
    household_key,
    date_key,
    count(*) as row_count
from {{ ref('fact_energy_daily') }}
group by
    household_key,
    date_key
having count(*) > 1