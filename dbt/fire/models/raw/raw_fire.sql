WITH raw_fire AS (
    SELECT 
        *
    FROM {{ source('postgres', 'fire_incidents') }}
)
SELECT 
    *
FROM raw_fire
