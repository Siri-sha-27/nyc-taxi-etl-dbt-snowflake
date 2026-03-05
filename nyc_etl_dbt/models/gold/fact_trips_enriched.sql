{{ config(materialized='table') }}

select
    t.*,

    -- weather enrichment
    w.temperature_2m,
    w.precipitation,
    w.weathercode,

    -- holiday enrichment
    coalesce(h.is_holiday, false) as is_holiday,
    h.holiday_name

from {{ ref('taxi_trips_clean') }} t
left join {{ ref('weather_hourly') }} w
    on t.pickup_hour = w.hour_ts
left join {{ ref('dim_holidays') }} h
    on t.pickup_date = h.holiday_date