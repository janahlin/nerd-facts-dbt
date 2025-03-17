WITH types AS (
    SELECT DISTINCT
        primary_type,
        COUNT(*) AS num_pokemon
    FROM "nerd_facts"."public"."stg_pokeapi"
    GROUP BY primary_type
)
SELECT * FROM types