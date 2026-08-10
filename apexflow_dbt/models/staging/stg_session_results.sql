{{ config(materialized='view') }}

SELECT
    CONCAT(CAST(session_key AS STRING), '-', CAST(driver_number AS STRING)) AS result_id,
    session_key,
    meeting_key,
    driver_number,
    position AS finishing_position,
    number_of_laps,
    points,
    duration AS race_duration_seconds,
    gap_to_leader,
    SAFE_CAST(gap_to_leader AS FLOAT64) AS gap_to_leader_seconds,
    dnf,
    dns,
    dsq,
    CASE
        WHEN dsq THEN 'DSQ'
        WHEN dns THEN 'DNS'
        WHEN dnf THEN 'DNF'
        ELSE 'Classified'
    END AS result_status
FROM {{ source('apexflow_bronze', 'session_results_raw') }}
WHERE position IS NOT NULL
