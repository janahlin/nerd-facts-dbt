WITH starships AS (
    SELECT * FROM "nerd_facts"."public"."stg_swapi"
)
SELECT
    id,
    starship_name,
    model,
    manufacturer,
    max_speed,
    crew,
    passengers
FROM starships