SELECT
    aircraft_code,
    COUNT(aircraft_code) AS aircraft_seats
FROM
    {{ ref('stg_flights__seats') }}
GROUP BY aircraft_code;