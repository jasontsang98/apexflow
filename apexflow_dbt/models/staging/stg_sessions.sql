{{ config(materialized='view') }}

SELECT
    session_key,
    meeting_key,
    session_name,
    session_type,
    date_start AS session_start,
    date_end AS session_end,
    year AS season
FROM {{ source('apexflow_bronze', 'sessions_metadata') }}
