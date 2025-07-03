{{
    config(
      materialized = 'table',
      schema = generate_schema_name('marts')
    )
}}
WITH flights_agg AS (
    SELECT
        f."flight_id",
        COUNT(tf."ticket_no") AS "ticket_no_cnt",
        SUM(tf."amount") AS "amount_sum"
    FROM
        {{ ref('fct_flights') }} f
    LEFT JOIN {{ ref('fct_ticket_flights') }} tf USING("flight_id")
    GROUP BY f."flight_id"
),
flights_agg_1 AS (
    SELECT
        f.*,
        f1."ticket_no_bp_cnt"
    FROM flights_agg f
    INNER JOIN (
        SELECT
            f."flight_id",
            COUNT(bp."ticket_no") AS "ticket_no_bp_cnt"
        FROM flights_agg f
        LEFT JOIN {{ ref('fct_boarding_passes') }} bp USING("flight_id")
        GROUP BY f."flight_id"
    ) f1 USING("flight_id")
),
aircraft_seats_cnt AS (
    SELECT
        a."aircraft_code" AS "aircraft_code",
        COUNT(s."seat_no") AS "seat_no_cnt"
    FROM {{ ref('stg_flights__aircrafts') }} a
    LEFT JOIN {{ ref('stg_flights__seats') }} s USING("aircraft_code")
    GROUP BY a."aircraft_code"
),
flights_with_agg AS (
    SELECT
        f.*,
        fa."ticket_no_cnt",
        fa."amount_sum",
        fa."ticket_no_bp_cnt",
        a."seat_no_cnt" - fa."ticket_no_cnt" AS "ticket_no_sold_cnt"
    FROM {{ ref('fct_flights') }} f
    LEFT JOIN flights_agg_1 fa USING("flight_id")
    LEFT JOIN aircraft_seats_cnt a USING("aircraft_code")
)
SELECT
    f."departure_airport" AS "Departure_Airport_Code",
    a."airport_name" AS "Departure_Airport_Name",
    a."city" AS "Departure_Airport_City",
    a."coordinates" AS "Departure_Airport_Coordinates",
    f."arrival_airport" AS "Arrival_Airport_Code",
    a2."city" AS "Arrival_Airport_City",
    a2."airport_name" AS "Arrival_Airport_Name",
    a2."coordinates" AS "Arrival_Airport_Coordinates",
    f."status" AS "Flight_status",
    f."aircraft_code" AS "Aircraft_code",
    ac."model" AS "Aircraft_model",
    f."scheduled_departure" AS "Scheduled_departure_date",
    f."flight_no" AS "Flight_no",
    f."flight_id" AS "Flight_id",
    f."ticket_no_cnt" AS "Ticket_flights_purchased",
    f."amount_sum" AS "Boarding_passes_issued",
    f."ticket_no_bp_cnt" AS "Ticket_flights_amount",
    f."ticket_no_sold_cnt" AS "Ticket_flights_no_sold"
FROM 
    flights_with_agg f
LEFT JOIN {{ ref('stg_flights__airports') }} a
ON f."departure_airport" = a."airport_code"
LEFT JOIN {{ ref('stg_flights__airports') }} a2
ON f."arrival_airport" = a2."airport_code"
LEFT JOIN {{ ref('stg_flights__aircrafts') }} ac
ON f."aircraft_code" = ac."aircraft_code"