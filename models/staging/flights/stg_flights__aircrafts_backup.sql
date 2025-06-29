{{
    config(
        materialized = 'table',
        pre_hook = "
            {% set current_date = run_started_at.astimezone(modules.pytz.timezone('Europe/Moscow')).strftime('%Y_%m_%d_%H%M%S') %}
            {% set backup_relation = api.Relation.create(
                    database = this.database,
                    schema = this.schema,
                    identifier = this.identifier ~ '_' ~ current_date,
                    type = 'table'
                ) 
            %}
            {% do adapter.drop_relation(backup_relation) %}
            {% do adapter.rename_relation(this, backup_relation) %}
        "
    )
}}
SELECT
    aircraft_code,
    model,
    "range"
FROM
    {{ source('demo_src', 'aircrafts') }}