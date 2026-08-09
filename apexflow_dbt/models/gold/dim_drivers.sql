{{ config(materialized='table') }}

WITH available_drivers AS (
    SELECT DISTINCT session_key, driver_number
    FROM {{ ref('fct_lap_leaderboard') }}
)

SELECT drivers.*
FROM {{ ref('stg_drivers') }} AS drivers
JOIN available_drivers AS available
    USING (session_key, driver_number)
