{{
    config(
      materialized = 'table'
      )
}}
select
  "flight_id",
  "flight_no",
  "scheduled_departure",
  "scheduled_departure_local",
  "scheduled_arrival",
  "scheduled_arrival_local",
  "scheduled_duration",
  "departure_airport",
  "departure_airport_name",
  "departure_city",
  "arrival_airport",
  "arrival_airport_name",
  "arrival_city",
  "status",
  "aircraft_code",
  "actual_departure",
  "actual_departure_local",
  "actual_arrival",
  "actual_arrival_local",
  "actual_duration"
from {{ source('demo_src', 'flights_v') }}