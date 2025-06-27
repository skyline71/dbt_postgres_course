{{
    config(
    materialized = 'table',
    )
}}
SELECT
    passenger_id
FROM
    {{ ref('passengers') }}