SELECT
    total_amount 
FROM
    {{ ref('stg_flights__bookings') }}
WHERE
    total_amount > 10000000
    OR total_amount <= 0