{{
    config(
      materialized = 'incremental',
      incremental_strategy = 'append'
    )
}}
select
  "flight_id",
  "flight_no",
  "scheduled_departure",
  "scheduled_arrival",
  "departure_airport",
  "arrival_airport",
  "status",
  "aircraft_code",
  "actual_departure",
  "actual_arrival"
from {{ source('demo_src', 'flights') }}
{% if is_incremental() %}
where
 "scheduled_arrival" >= (SELECT MAX("scheduled_arrival") FROM {{ this }})
{% endif %}