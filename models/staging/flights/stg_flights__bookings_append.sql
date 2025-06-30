{{
    config(
      materialized = 'incremental',
      incremental_strategy = 'append',
      tags = ['bookings']
      )
}}
select
  "book_ref",
  "book_date",
  "total_amount"
from {{ source('demo_src', 'bookings') }}
{% if is_incremental() %}
where
 {{ bookref_to_bigint("book_ref") }} > (SELECT MAX({{ bookref_to_bigint("book_ref") }}) FROM {{ this }})
{% endif %}