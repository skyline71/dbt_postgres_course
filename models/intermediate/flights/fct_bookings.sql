{{
    config(
        materialized = 'table'
    )
}}
select
    {{ show_columns_relation(ref('stg_flights__bookings')) }},
    {{ dbt_utils.generate_surrogate_key(['book_ref']) }} AS book_ref_surrogate_key
from
    {{ ref('stg_flights__bookings') }}
{{ limit_data_dev('book_date', days=5000) }}