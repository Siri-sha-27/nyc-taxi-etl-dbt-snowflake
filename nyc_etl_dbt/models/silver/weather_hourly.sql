{{ config(materialized='table') }}

with src as (
    select payload
    from NYC_ETL.RAW.weather_2024_01_raw
),

times as (
    select
        f.index as idx,
        try_to_timestamp_ntz(f.value::string) as hour_ts
    from src,
    lateral flatten(input => payload:hourly:time) f
),

temp as (
    select
        f.index as idx,
        f.value::float as temperature_2m
    from src,
    lateral flatten(input => payload:hourly:temperature_2m) f
),

precip as (
    select
        f.index as idx,
        f.value::float as precipitation
    from src,
    lateral flatten(input => payload:hourly:precipitation) f
),

code as (
    select
        f.index as idx,
        f.value::int as weathercode
    from src,
    lateral flatten(input => payload:hourly:weathercode) f
)

select
    t.hour_ts,
    te.temperature_2m,
    pr.precipitation,
    co.weathercode
from times t
left join temp te using (idx)
left join precip pr using (idx)
left join code co using (idx)
where t.hour_ts is not null