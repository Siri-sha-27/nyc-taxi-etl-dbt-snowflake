-- Fail if passenger_count null rate exceeds 5%
with stats as (
  select
    count(*) as total_rows,
    sum(case when passenger_count is null then 1 else 0 end) as null_rows
  from {{ ref('taxi_trips_clean') }}
)
select *
from stats
where (null_rows * 1.0 / nullif(total_rows, 0)) > 0.05