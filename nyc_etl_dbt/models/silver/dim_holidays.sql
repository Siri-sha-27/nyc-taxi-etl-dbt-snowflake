{{ config(materialized='table') }}

select
    date::date as holiday_date,
    holiday_name::string as holiday_name,
    true as is_holiday
from NYC_ETL.RAW.us_holidays_2024