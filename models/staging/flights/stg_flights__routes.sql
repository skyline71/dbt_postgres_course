{{
    config(
      materialized = 'table'
      )
}}
select
  "flight_no",
  "departure_airport",
  "departure_airport_name",
  "departure_city",
  "arrival_airport",
  "arrival_airport_name",
  "arrival_city",
  "aircraft_code",
  "duration",
  "days_of_week"

from {{ source('demo_src', 'routes') }}