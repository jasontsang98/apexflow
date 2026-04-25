{{ config(materialized='view') }}

SELECT
    session_key,
    driver_number,
    lap_number,
    date AS pit_entry_timestamp,
    -- Renaming duration to be more descriptive
    stop_duration AS pit_stop_duration_seconds
FROM {{ source('apexflow_bronze', 'pits_raw') }}