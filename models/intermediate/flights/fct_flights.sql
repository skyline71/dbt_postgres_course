{#
{% set flight_statuses = dbt_utils.get_column_values(
    table=ref('stg_flights__flights'),
    column='status'
) %}

{% set flight_statuses_joint = flight_statuses | join(', ') %}
{% do log('unique flight statuses: ' ~ flight_statuses_joint) %}
#}

{{
    config(
        materialized = 'table'
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
    "actual_arrival",
    case
        when actual_departure is not null and actual_arrival is not null
        then actual_arrival - actual_departure
        else INTERVAL '0 seconds'
    end as actual_duration_flight
from
    {{ ref('stg_flights__flights') }}
