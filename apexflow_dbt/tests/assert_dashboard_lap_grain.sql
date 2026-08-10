SELECT session_key, driver_number, lap_number
FROM {{ ref('fct_dashboard_laps') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) != 1
