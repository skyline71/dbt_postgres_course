SELECT
    *
FROM
    {{ safe_select(table_name='fct_bookings_abc') }}
