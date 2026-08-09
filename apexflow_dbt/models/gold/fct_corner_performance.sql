{{
    config(
        materialized='table',
        cluster_by=['session_key', 'driver_number']
    )
}}

-- This is the lap-level V-min baseline for future corner analysis. True
-- per-corner metrics will require a circuit corner reference keyed by location.
SELECT
    session_key,
    driver_number,
    lap_number,
    telemetry_timestamp AS v_min_timestamp,
    speed AS lap_v_min,
    n_gear AS v_min_gear,
    throttle AS v_min_throttle,
    brake AS v_min_brake,
    x,
    y,
    tire_compound,
    current_tire_age,
    track_flag,
    track_temperature,
    is_raining
FROM {{ ref('fct_telemetry_enriched') }}
WHERE
    lap_number IS NOT NULL
    AND speed IS NOT NULL
    AND x IS NOT NULL
    AND y IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_key, driver_number, lap_number
    ORDER BY speed ASC, telemetry_timestamp ASC
) = 1
