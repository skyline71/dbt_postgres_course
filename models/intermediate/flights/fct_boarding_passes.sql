{{
    config(
      materialized = 'table'
      )
}}
select
  "ticket_no",
  "flight_id",
  "boarding_no",
  "seat_no"
from {{ ref('stg_flights__boarding_passes') }}