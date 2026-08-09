{{ config(materialized='view') }}

SELECT
    CONCAT(CAST(session_key AS STRING), '-', CAST(driver_number AS STRING)) AS driver_session_id,
    session_key,
    driver_number,
    broadcast_name,
    full_name,
    name_acronym,
    team_name,
    CONCAT('#', team_colour) AS team_colour_hex
FROM {{ source('apexflow_bronze', 'driver_raw') }}
