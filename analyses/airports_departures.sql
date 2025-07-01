WITH airports_statses_agg AS (
    SELECT
        departure_airport,
        {{ dbt_utils.pivot(
            'status',
            dbt_utils.get_column_values(ref('fct_flights'), 'status'),
            agg='sum'
        ) }}
    FROM
        {{ ref('fct_flights') }}
    GROUP BY departure_airport
)
SELECT
    *
FROM
    airports_statses_agg
WHERE "Departed" > 0