{{ config(materialized='view') }}

SELECT
    session_key,
    driver_number,
    date AS location_timestamp,
    x,
    y,
    z
FROM {{ source('apexflow_bronze', 'location_raw') }}