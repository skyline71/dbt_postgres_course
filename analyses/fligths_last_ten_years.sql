{% set current_date = run_started_at|string|truncate(10, True, "") %}
{% set current_year = current_date|truncate(4, True, "")|int %}
{% set prev_year = current_year - 10 %}

SELECT 
    COUNT(*)
FROM
    {{ ref('fct_flights') }}
WHERE
    scheduled_departure between '{{ current_date }}' and '{{ current_date|replace(current_year, prev_year) }}';

{% set relation_exists = load_relation(ref('stg_flights__flights')) is not none %}
{% if relation_exists %}
      {%- set columns = adapter.get_columns_in_relation(load_relation(ref('stg_flights__flights'))) -%}
{% else %}
      {%- set columns = [] -%}
{% endif %}

{% for column in columns %}
  {{ "Column: " ~ column }}
{% endfor %}