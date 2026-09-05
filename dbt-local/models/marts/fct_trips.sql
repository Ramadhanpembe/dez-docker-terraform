{{
    config(
        materialized='incremental',
        unique_key='trip_id',
    )
}}

select
    -- Trip identifiers
    trips.trip_id,
    trips.vendorid,
    trips.service_type,
    trips.ratecodeid,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_locationid,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_locationid,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    {{ get_trip_duration_minutes('trips.pickup_datetime', 'trips.dropoff_datetime') }} as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from {{ ref('int_trips') }} as trips
left join {{ ref('dim_zones') }} as pz
    on trips.pickup_locationid = pz.location_id
left join {{ ref('dim_zones') }} as dz
    on trips.dropoff_locationid = dz.location_id

{% if is_incremental() %}
    where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
