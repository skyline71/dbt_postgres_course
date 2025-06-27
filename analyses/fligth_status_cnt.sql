{% set flights_query %}
SELECT DISTINCT
    "status"
FROM
    {{ ref('stg_flights__flights') }}
{% endset %}  

{% set flights_query_result = run_query(flights_query) %}
{% if execute %}
    {% set important_stasuses = flights_query_result.columns[0].values() %}
{% else %}
    {% set important_stasuses = [] %}
{% endif %}

SELECT
    {% for important_status in important_stasuses %}
    SUM(CASE WHEN status = '{{ important_status }}' THEN 1 ELSE 0 END) AS "status_{{ important_status }}"
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
FROM
    {{ ref('stg_flights__flights') }}