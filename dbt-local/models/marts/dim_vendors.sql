with vendors as (
    select distinct vendorid
    from {{ ref('stg_green_tripdata') }}
)

select 
    vendorid,
    case 
        when vendorid = 1 then 'Creative Mobile Technologies, LLC'
        when vendorid = 2 then 'VeriFone Inc.'
    end as vendor_name
from vendors
