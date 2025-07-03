{{
    config(
        severity = 'error',
        error_if = '>100',
        warn_if = 'between 50 and 100'
    )
}}
SELECT
    "book_ref",
    COUNT("book_ref")
FROM
    {{ ref('stg_flights__tickets') }}
GROUP BY "book_ref"
HAVING COUNT("book_ref") >= 5