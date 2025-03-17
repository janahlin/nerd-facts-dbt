
  
    

  create  table "nerd_facts"."public"."dim_locations__dbt_tmp"
  
  
    as
  
  (
    /*
  Model: dim_locations (Simplified)
  Description: Basic location dimension table with consistent TEXT types
*/

WITH sw_planets_minimal AS (
    SELECT
        'sw_' || id AS source_id,
        'star_wars' AS universe,
        planet_name AS location_name,
        'Planet' AS location_type,
        diameter::TEXT,  -- Cast to TEXT
        rotation_period::TEXT,  -- Cast to TEXT
        orbital_period::TEXT,  -- Cast to TEXT
        gravity,
        population::TEXT,  -- Cast to TEXT
        climate,
        terrain,
        surface_water::TEXT  -- Cast to TEXT
    FROM "nerd_facts"."public"."stg_swapi_planets"
),

pokemon_regions_minimal AS (
    -- Extract distinct regions from Pokémon data with minimal fields
    SELECT DISTINCT
        'pkm_' || md5(cast(coalesce(cast(region as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS source_id,
        'pokemon' AS universe,
        region AS location_name,
        'Region' AS location_type,
        NULL::TEXT AS diameter,  -- Cast NULL to TEXT
        NULL::TEXT AS rotation_period,  -- Cast NULL to TEXT
        NULL::TEXT AS orbital_period,  -- Cast NULL to TEXT
        '1 standard' AS gravity,
        CASE  
            WHEN region = 'Kanto' THEN '10000000'
            ELSE '5000000'
        END AS population,  -- String literals
        'varied' AS climate,
        'varied' AS terrain,
        '30' AS surface_water  -- String literal
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE region IS NOT NULL
),

netrunner_locations_minimal AS (
    -- Create minimal Netrunner locations with text values
    SELECT * FROM (VALUES
        ('nr_1001', 'netrunner', 'New Angeles', 'Megacity', NULL::TEXT, NULL::TEXT, NULL::TEXT, 'high', '500000000', 'temperate', 'urban', '10'),
        ('nr_1002', 'netrunner', 'The Moon', 'Colony', '3474', '27.3', '27.3', 'low', '5000000', 'artificial', 'lunar', '0')
    ) AS v(source_id, universe, location_name, location_type, diameter, rotation_period, 
           orbital_period, gravity, population, climate, terrain, surface_water)
)

-- Rest of query remains the same
SELECT
    md5(cast(coalesce(cast(universe as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS location_key,
    source_id,
    universe,
    location_name,
    location_type,
    diameter,
    rotation_period,
    orbital_period,
    gravity,
    population,
    climate,
    terrain,
    surface_water,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM (
    SELECT * FROM sw_planets_minimal
    UNION ALL
    SELECT * FROM pokemon_regions_minimal
    UNION ALL
    SELECT * FROM netrunner_locations_minimal
) AS all_locations
ORDER BY universe, location_name
  );
  