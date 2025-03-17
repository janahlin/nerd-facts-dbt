WITH manufacturers AS (
    SELECT DISTINCT
        manufacturer,
        COUNT(*) AS num_starships
    FROM "nerd_facts"."public"."stg_swapi"
    GROUP BY manufacturer
)
SELECT * FROM manufacturers