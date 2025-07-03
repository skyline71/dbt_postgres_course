{% test timezone_pattern(model, column_name) %}
    SELECT
        {{ column_name }}
    FROM
        {{ model }}
    WHERE 
        TRIM({{ column_name }}) NOT LIKE '%/%'
{% endtest %}