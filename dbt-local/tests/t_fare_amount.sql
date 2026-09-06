select 
    trip_id,
    fare_amount
from {{ ref('fct_trips') }}
where fare_amount <= 0