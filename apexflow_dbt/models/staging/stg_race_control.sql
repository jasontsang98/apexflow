{{ config(materialized='view') }}

SELECT
    session_key,
    date AS event_timestamp,
    driver_number, -- Note: This can be NULL for global flags like Safety Car
    category,
    flag,
    message,
    lap_number
FROM {{ source('apexflow_bronze', 'race_control_raw') }}
-- We only care about flags and racing incidents for telemetry analysis
WHERE category IN ('Flag', 'SafetyCar', 'Drs')