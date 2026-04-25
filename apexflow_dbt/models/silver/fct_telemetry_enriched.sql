{{ config(materialized='table') }}

WITH combined_stream AS (
    -- Telemetry Rows
    SELECT 
        session_key,
        driver_number,
        lap_number,
        telemetry_timestamp AS ts,
        speed,
        rpm,
        n_gear,
        throttle,
        brake,
        drs,
        is_pit_out_lap,
        lap_duration,
        'telemetry' AS record_type,
        CAST(NULL AS FLOAT64) AS air_temp,
        CAST(NULL AS FLOAT64) AS track_temp,
        CAST(NULL AS BOOL) AS raining, -- Matches the BOOL type of weather.is_raining
        CAST(NULL AS STRING) AS track_flag,
        CAST(NULL AS FLOAT64) AS x,
        CAST(NULL AS FLOAT64) AS y
    FROM {{ ref('fct_telemetry_laps') }}

    UNION ALL

    -- Weather Rows
    SELECT
        session_key,
        NULL, NULL, -- driver_number and lap_number
        weather_timestamp AS ts,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, -- Telemetry sensors
        'weather' AS record_type,
        air_temperature,
        track_temperature,
        is_raining,
        NULL, --placeholder for race control flags
        NULL, NULL
    FROM {{ ref('stg_weather') }}

    UNION ALL

    -- Race Control Rows
    SELECT
        session_key,
        NULL, NULL,
        event_timestamp AS ts,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        'race_control' AS record_type,
        NULL, NULL, NULL,
        flag, -- We bring in the flag (e.g., YELLOW, GREEN, VSC)
        NULL, NULL
    FROM {{ ref('stg_race_control') }}
    WHERE category IN ('Flag', 'SafetyCar')
        AND flag != 'BLUE'-- Only smear global status changes

    UNION ALL

    -- Location Rows
    SELECT
        session_key,
        driver_number,
        NULL,
        location_timestamp AS ts,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, -- Telemetry placeholders
        'location' AS record_type,
        NULL, NULL, NULL, NULL, -- Weather/Flag placeholders
        x, y
    FROM {{ ref('stg_location') }}
),

interpolated AS (
    SELECT
        *,
        -- Smear Weather
        LAST_VALUE(air_temp IGNORE NULLS) OVER (
            PARTITION BY session_key ORDER BY ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS air_temp_filled,
        LAST_VALUE(track_temp IGNORE NULLS) OVER (
            PARTITION BY session_key ORDER BY ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS track_temp_filled,
        LAST_VALUE(raining IGNORE NULLS) OVER (
            PARTITION BY session_key ORDER BY ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS raining_filled,
        -- Smear race flags
        LAST_VALUE(track_flag IGNORE NULLS) OVER (
            PARTITION BY session_key ORDER BY ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS track_flag_filled,
        -- Smear Driver Specific Location
        LAST_VALUE(x IGNORE NULLS) OVER (
            PARTITION BY session_key, driver_number ORDER BY ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS x_filled,
        LAST_VALUE(y IGNORE NULLS) OVER (
            PARTITION BY session_key, driver_number ORDER BY ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS y_filled
    FROM combined_stream
),

telemetry_with_context AS (
    SELECT
        i.session_key,
        i.driver_number,
        i.lap_number,
        i.ts AS telemetry_timestamp,
        i.speed,
        i.rpm,
        i.n_gear,
        i.throttle,
        i.brake,
        i.drs,
        i.is_pit_out_lap,
        i.lap_duration,
        i.air_temp_filled AS air_temperature,
        i.track_temp_filled AS track_temperature,
        i.raining_filled AS is_raining,
        COALESCE(i.track_flag_filled, 'GREEN') AS track_flag, -- Default to GREEN if no flag yet
        i.x_filled AS x,
        i.y_filled AS y
    FROM interpolated i
    WHERE i.record_type = 'telemetry'
)

SELECT
    t.*,
    -- Tire Info
    s.tire_compound,
    s.tyre_age_at_start + (t.lap_number - s.lap_start) AS current_tire_age,
    -- Pit Info
    p.pit_stop_duration_seconds
FROM telemetry_with_context t
LEFT JOIN (
    SELECT
        session_key,
        driver_number,
        lap_start,
        COALESCE(
            LEAD(lap_start) OVER (PARTITION BY session_key, driver_number ORDER BY lap_start) - 1, 
            lap_end
        ) AS clean_lap_end,
        tire_compound,
        tyre_age_at_start
    FROM {{ ref('stg_stints') }}
)s
    ON t.session_key = s.session_key
    AND t.driver_number = s.driver_number
    AND t.lap_number BETWEEN s.lap_start AND s.clean_lap_end
LEFT JOIN {{ ref('stg_pits') }} p
    ON t.session_key = p.session_key
    AND t.driver_number = p.driver_number
    AND t.lap_number = p.lap_number