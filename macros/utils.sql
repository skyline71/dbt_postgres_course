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
        
        {# вывод названия таблицы / NULL #}

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


{% macro get_graph_nodes() %}
    {% if execute %}

    {# Создание словаря списков узлов по типам (модель, сид, снапшот) #}

    {% set node_resource_types = ['model', 'seed', 'snapshot'] %}
    {% set resource_types_dict = {} %}
    {% for node in graph.nodes.values() -%}
        {% if node.resource_type in node_resource_types %}
            {% if node.resource_type not in resource_types_dict %}
                {% do resource_types_dict.update({node.resource_type: []}) %}
            {% endif %}
            {% do resource_types_dict[node.resource_type].append(node.name) %}
        {% endif %}
    {% endfor %}

    {# Создание словаря с агрегацией типов по количеству значений #}

    {% set result = {} %}
    {% for key, types_list in resource_types_dict.items() %}
        {% do result.update({key: types_list | length }) %}
    {% endfor %}

    {# Вывод результатов в логи #}

    {% do log('Всего в проекте:', True) %}
    {% for key, cnt in result.items() %}
        {% do log('- ' ~ cnt ~ ' ' ~ key, True) %}
    {% endfor %}

    {{ return(result) }}
    {% endif %}
{% endmacro %}

{% macro check_dependencies() %}
    {% if execute %}

        {# Создание словаря количества зависимостей моделей #}

        {% set cnt_types_dict = get_graph_nodes() %}
        {% set dependencies_cnt_dict = {} %}

        {# Заполнение словаря списком количества зависимостей от каждого объекта (макрос, модель и т.д.) #}

        {% for node in graph.nodes.values() -%}
            {% if node.resource_type in cnt_types_dict.keys() %}
                {# do log(node.depends_on, True) #}
                {% for key, value in node.depends_on.items() %}
                    {% if node.name not in dependencies_cnt_dict %}
                        {% do dependencies_cnt_dict.update({node.name: []}) %}
                    {% endif %}
                    {% do dependencies_cnt_dict[node.name].append(value | length) %}
                {% endfor %}
            {% endif %}
        {% endfor %}

        {# Вывод результатов вычислений в логи с предупреждением переполнения количества зависимостей у модели #}

        {% for key, cnt in dependencies_cnt_dict.items() %}
            {% if cnt | sum > 1 %}
                {{ exceptions.warn('⚠️ Модель ' ~ key ~ ' зависит от ' ~ cnt | sum ~ ' объектов!') }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}