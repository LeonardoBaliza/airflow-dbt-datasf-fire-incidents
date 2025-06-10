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
)
-- Aggregate fire incidents by date, battalion, neighborhood, and supervisor district
SELECT 
    CAST(incident_date AS DATE)         AS incident_date,         -- Date of the incident
    battalion                           AS battalion,             -- Battalion identifier
    neighborhood_district               AS neighborhood_district, -- Neighborhood district
    supervisor_district                 AS supervisor_district,   -- Supervisor district
    COUNT(id)                           AS incident_count         -- Number of incidents
FROM 
    raw_fire
GROUP BY
    CAST(incident_date AS DATE),        -- Group by date
    battalion,                          -- Group by battalion
    neighborhood_district,              -- Group by neighborhood
    supervisor_district                 -- Group by supervisor district
ORDER BY
    CAST(incident_date AS DATE) DESC,   -- Most recent dates first
    battalion ASC,                      -- Sort battalion ascending
    neighborhood_district ASC,          -- Sort neighborhood ascending
    supervisor_district ASC             -- Sort supervisor district ascending