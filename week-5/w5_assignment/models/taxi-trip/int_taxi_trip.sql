{{
  config(
    materialized='view'
  )
}}

WITH source AS (
    SELECT
        VendorID,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        RatecodeID,
        PULocationID,
        DOLocationID,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        Price as usd_idr_rate,
        total_amount*Price as total_amount_idr,
        payment_type,
        congestion_surcharge
    FROM
        {{ source('raw_nyc_taxi_trip', 'nyc_taxi_trip') }} tx
    JOIN
        {{ source('usd_idr_rate', 'usd_idr_rate') }} d
    ON EXTRACT(DATE FROM lpep_pickup_datetime) = d._Date_
    WHERE
        RatecodeID IS NOT NULL AND
        (total_amount > 0 OR payment_type = 3) AND
        payment_type IS NOT NULL
)

SELECT * FROM source