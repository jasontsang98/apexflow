SELECT session_key, finishing_position
FROM {{ ref('fct_race_results') }}
GROUP BY 1, 2
HAVING COUNT(*) != 1 OR finishing_position < 1
