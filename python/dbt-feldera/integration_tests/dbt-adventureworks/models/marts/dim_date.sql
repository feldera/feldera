with stg_date as (
    select * from {{ ref('calendar_date') }}
)

select
    {{ generate_surrogate_key(['stg_date.date_day']) }} as date_key,
    stg_date.*
from stg_date
