-- CTE to select relevant columns from the raw_fire table
WITH raw_fire AS (
    SELECT
        id
        , battalion
        , neighborhood_district
        , supervisor_district
        , incident_date
    FROM
        {{ ref('raw_fire') }}
), calendar AS (
    -- CTE to select holiday dates
    SELECT 
        date
        , is_holiday
    FROM 
        {{ source('calendar', 'calendar') }}
    WHERE 1 = 1
        AND is_holiday = TRUE  -- Filter for holidays only
)
-- Aggregate fire incidents by date, battalion, neighborhood, and supervisor district
SELECT 
    CAST(raw_fire.incident_date AS DATE)         AS incident_date,         -- Date of the incident
    raw_fire.battalion                           AS battalion,             -- Battalion identifier
    raw_fire.neighborhood_district               AS neighborhood_district, -- Neighborhood district
    raw_fire.supervisor_district                 AS supervisor_district,   -- Supervisor district
    COUNT(raw_fire.id)                           AS incident_count         -- Number of incidents
FROM 
    raw_fire
INNER JOIN
    calendar
    ON CAST(raw_fire.incident_date AS DATE) = calendar.date
GROUP BY
    CAST(raw_fire.incident_date AS DATE),        -- Group by date
    raw_fire.battalion,                          -- Group by battalion
    raw_fire.neighborhood_district,              -- Group by neighborhood
    raw_fire.supervisor_district                 -- Group by supervisor district
ORDER BY
    CAST(raw_fire.incident_date AS DATE) DESC,   -- Most recent dates first
    raw_fire.battalion ASC,                      -- Sort battalion ascending
    raw_fire.neighborhood_district ASC,          -- Sort neighborhood ascending
    raw_fire.supervisor_district ASC             -- Sort supervisor district ascending