{{
  config(
    materialized='view'
  )
}}

SELECT
    {{extract('YEAR', 'lpep_pickup_datetime')}} AS year,
    {{extract('MONTH', 'lpep_pickup_datetime')}} AS month,
    SUM(passenger_count) AS total_passenger
FROM
    {{ ref('int_taxi_trip') }}
GROUP BY
  year, month
ORDER BY
  year, month