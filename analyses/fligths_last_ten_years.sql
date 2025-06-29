{% set current_date = '{{ run_started_at|string|truncate(10, True, "") }}' %}

SELECT 
    COUNT(*)
FROM
    {{ ref('fct_flights') }}
WHERE
    scheduled_departure::date between (current_date::date - '10 years'::INTERVAL)::date and current_date::date;