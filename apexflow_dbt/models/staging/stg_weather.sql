{{ config(materialized='view') }}

SELECT
    session_key,
    date AS weather_timestamp,
    air_temperature,
    track_temperature,
    humidity,
    pressure,
    wind_direction,
    wind_speed,
    rainfall AS is_raining 
FROM {{ source('apexflow_bronze', 'weather_raw') }}