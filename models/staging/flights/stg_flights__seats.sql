{{
    config(
      materialized = 'table'
      )
}}
select
  "aircraft_code",
  "seat_no",
  "fare_conditions",
  1 as ss
from {{ source('demo_src', 'seats') }}