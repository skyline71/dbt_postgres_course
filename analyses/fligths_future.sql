SELECT 
    COUNT(*)
FROM
    {{ ref('fct_flights') }}
WHERE
    scheduled_departure >= '{{ run_started_at|string|truncate(10, True, "") }}'