with unioned as (
    select * from {{ ref('int_trips_union') }}
),

payment_types as (
    select * from {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched as (
    select
        {{ 
            dbt_utils.generate_surrogate_key(
                [
                    'u.vendorid', 
                    'u.pickup_datetime', 
                    'u.pickup_locationid', 
                    'u.service_type'
                ]
            ) 
        }} as trip_id,

        -- Identifiers
        u.vendorid,
        u.service_type,
        u.ratecodeid,

        -- Location IDs
        u.pickup_locationid,
        u.dropoff_locationid,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description

    from unioned u
    left join payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
),

-- PostgreSQL replacement for QUALIFY clause
deduplicated as (
    select
        *,
        row_number() over(
            partition by vendorid, pickup_datetime, pickup_locationid, service_type
            order by dropoff_datetime
        ) as rn
    from cleaned_and_enriched
)

select
    trip_id,
    vendorid,
    service_type,
    ratecodeid,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    payment_type_description
from deduplicated
where rn = 1

-- not supported by postgres
{# select * from cleaned_and_enriched
qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1 #}
