{% macro bookref_to_bigint(bookref) %}
    ('0x' || book_ref)::bigint
{% endmacro %}

{% macro safe_select(table_name) %}

    {% if execute %}
    
        {# формирование скрипта проверки существования таблицы #}
        
        {% set check_table_exists %}
        SELECT
            CASE
                WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
                WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
            END AS RELATION_TYPE,
            CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) as RELATION_NAME
        FROM
            {{ target.database }}.INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_SCHEMA = '{{ target.schema }}'
            AND UPPER(TABLE_NAME) = '{{ table_name.upper() }}';
        {% endset %}
        
        {% do log(check_table_exists) %}
        
        {% set is_exists = run_query(check_table_exists).columns[1].values() %}
        
        {# удаление лишних таблиц и вьюх / вывод скрипта удаления #}

        {% if is_exists %}
            {% for table in is_exists %}
                {% do log(table, True) %}
                {{ table }}
            {% endfor %}
        {% else %}
            {{ '(SELECT NULL)' }}
        {% endif %}
    
    {% endif %}

{% endmacro %}

{% macro show_columns_relation(source_relation) %}
    {{ log("Source Relation: " ~ source_relation, True) }}

    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}

    {% for column in columns %}
        {{ log("Column: " ~ column, True) }}
        {{ column.column ~ ' AS ' ~ column.column }}
        {%- if not loop.last -%}
            ,
        {%- endif %}
    {% endfor %}
{% endmacro %}