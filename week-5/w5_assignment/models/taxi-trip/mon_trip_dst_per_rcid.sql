{{
  config(
    materialized='view'
  )
}}

SELECT
    {{extract('YEAR', 'lpep_pickup_datetime')}} AS year,
    {{extract('MONTH', 'lpep_pickup_datetime')}} AS month,
    CASE
      WHEN RatecodeID = 1 THEN 'Standard rate'
      WHEN RatecodeID = 2 THEN 'JFK'
      WHEN RatecodeID = 3 THEN 'Newark'
      WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
      WHEN RatecodeID = 5 THEN 'Negotiated fare'
      WHEN RatecodeID = 6 THEN 'Group ride'
    END AS Ratecode_name,
    SUM(trip_distance) AS total_trip_distance
FROM
    {{ ref('int_taxi_trip') }}
GROUP BY
  year, month, Ratecode_name
ORDER BY
  year, month, Ratecode_name