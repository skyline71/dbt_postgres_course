{% test airport_code_pattern(model, column_name) %}
    SELECT
        {{ column_name }}
    FROM
        {{ model }}
    WHERE NOT
        LENGTH({{ column_name }}) = 3
        AND {{ column_name }} = UPPER({{ column_name }})
{% endtest %}