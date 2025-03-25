

/*
  Model: fact_starships (simplified)
  Description: Fact table for Star Wars starships with basic fields
*/

WITH starships AS (
    SELECT
        s.starship_id,
        s.starship_name,
        s.model,
        s.manufacturer,
        s.cost_in_credits,
        s.length,
        s.max_atmosphering_speed,
        s.crew,
        s.passengers,
        s.cargo_capacity,
        s.consumables,
        s.hyperdrive_rating,
        s.MGLT,
        s.starship_class
    FROM "nerd_facts"."public"."int_swapi_starships" s
    WHERE s.starship_id IS NOT NULL
),

-- Get film data for starships
starship_films AS (
    SELECT
        fs.starship_id,
        COUNT(DISTINCT fs.film_id) AS film_count,
        STRING_AGG(f.title, ', ' ORDER BY f.episode_id) AS film_names
    FROM "nerd_facts"."public"."int_swapi_films_starships" fs
    JOIN "nerd_facts"."public"."int_swapi_films" f ON fs.film_id = f.film_id
    GROUP BY fs.starship_id
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(s.starship_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS starship_key,
    
    -- Core identifiers
    s.starship_id,
    s.starship_name,
    s.model,
    s.manufacturer,
    
    -- Classification
    s.starship_class,
    
    -- Basic metrics with safe type handling
    CASE WHEN s.cost_in_credits::TEXT ~ '^[0-9]+$' THEN s.cost_in_credits::NUMERIC ELSE NULL END AS cost_credits,
    CASE WHEN s.length::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN s.length::NUMERIC ELSE NULL END AS length_m,
    CASE WHEN s.max_atmosphering_speed::TEXT ~ '^[0-9]+$' THEN s.max_atmosphering_speed::NUMERIC ELSE NULL END AS max_speed,
    CASE WHEN s.hyperdrive_rating::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN s.hyperdrive_rating::NUMERIC ELSE NULL END AS hyperdrive,
    CASE WHEN s.MGLT::TEXT ~ '^[0-9]+$' THEN s.MGLT::NUMERIC ELSE NULL END AS MGLT,
    
    -- Film appearances
    COALESCE(sf.film_count, 0) AS film_count,
    COALESCE(sf.film_names, 'None') AS film_appearances,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM starships s
LEFT JOIN starship_films sf ON s.starship_id = sf.starship_id
ORDER BY s.starship_id