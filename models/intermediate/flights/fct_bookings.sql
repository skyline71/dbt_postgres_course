{{
    config(
        materialized = 'table'
    )
}}
select
    {{ show_columns_relation(ref('stg_flights__bookings')) }}
from
    {{ ref('stg_flights__bookings') }}
{{ limit_data_dev('book_date', days=5000) }}