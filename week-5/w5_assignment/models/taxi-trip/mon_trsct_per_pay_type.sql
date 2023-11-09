{{
  config(
    materialized='view'
  )
}}

SELECT
    {{extract('YEAR', 'lpep_pickup_datetime')}} AS year,
    {{extract('MONTH', 'lpep_pickup_datetime')}} AS month,
    CASE
      WHEN payment_type = 1 THEN 'Credit card'
      WHEN payment_type = 2 THEN 'Cash'
      WHEN payment_type = 3 THEN 'No charge'
      WHEN payment_type = 4 THEN 'Dispute'
      WHEN payment_type = 5 THEN 'Unknown'
      WHEN payment_type = 6 THEN 'Voided trip'
    END AS payment_type_name,
    SUM(total_amount_idr) AS total_transaction_idr
FROM
    {{ ref('int_taxi_trip') }}
GROUP BY
  year, month, payment_type
ORDER BY
  year, month, payment_type