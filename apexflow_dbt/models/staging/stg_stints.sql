{{ config(materialized='view') }}

SELECT
    session_key,
    driver_number,
    stint_number,
    -- Normalizing the compound names (e.g., SOFT, MEDIUM, HARD)
    UPPER(compound) AS tire_compound,
    tyre_age_at_start,
    lap_start,
    -- Handle the case where a stint is ongoing (lap_end might be null)
    COALESCE(lap_end, 999) AS lap_end
FROM {{ source('apexflow_bronze', 'stints_raw') }}