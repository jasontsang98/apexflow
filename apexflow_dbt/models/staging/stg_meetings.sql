{{ config(materialized='view') }}

SELECT
    meeting_key,
    circuit_key,
    meeting_name,
    circuit_short_name,
    circuit_type,
    country_name,
    location,
    date_start AS meeting_start,
    date_end AS meeting_end,
    year AS season
FROM {{ source('apexflow_bronze', 'meetings_metadata') }}
