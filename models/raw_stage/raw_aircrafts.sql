{{
    config(
      materialized = 'table'
      )
}}
SELECT
    aircraft_code,
    model,
    "range",
    'bookings' as RECORD_SOURCE,
     now() as LOAD_DATE
FROM
    {{ source('demo_src', 'aircrafts') }}