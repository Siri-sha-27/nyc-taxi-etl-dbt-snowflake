{{ config(materialized='table') }}

with base as (
    select
        "VendorID"::number as vendor_id,
        to_timestamp_ntz("tpep_pickup_datetime" / 1000000)  as pickup_ts,
        to_timestamp_ntz("tpep_dropoff_datetime" / 1000000) as dropoff_ts,
        "passenger_count"::number as passenger_count,
        "trip_distance"::float as trip_distance,
        "RatecodeID"::number as ratecode_id,
        "store_and_fwd_flag"::string as store_and_fwd_flag,
        "PULocationID"::number as pu_location_id,
        "DOLocationID"::number as do_location_id,
        "payment_type"::number as payment_type,
        "fare_amount"::float as fare_amount,
        "extra"::float as extra,
        "mta_tax"::float as mta_tax,
        "tip_amount"::float as tip_amount,
        "tolls_amount"::float as tolls_amount,
        "improvement_surcharge"::float as improvement_surcharge,
        "total_amount"::float as total_amount,
        "congestion_surcharge"::float as congestion_surcharge,
        "Airport_fee"::float as airport_fee
    from NYC_ETL.RAW.taxi_tripdata_2024_01_v2
)

select
    vendor_id,
    pickup_ts  as tpep_pickup_datetime,
    dropoff_ts as tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,

    to_date(pickup_ts) as pickup_date,
    date_trunc('hour', pickup_ts) as pickup_hour,
    datediff('minute', pickup_ts, dropoff_ts) as trip_duration_minutes,

    iff(fare_amount < 0 or total_amount < 0, true, false) as is_refund,
    iff(trip_distance = 0, true, false) as is_zero_distance,
    iff(passenger_count is null, true, false) as is_passenger_unknown

from base
where
    pickup_ts is not null
    and dropoff_ts is not null
    and pickup_ts >= '2024-01-01'::timestamp_ntz
    and pickup_ts <  '2024-02-01'::timestamp_ntz
    and pickup_ts <= dropoff_ts