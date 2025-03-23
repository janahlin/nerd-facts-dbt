

/*
  Model: dim_sw_planets
  Description: Planet dimension table with enriched attributes and classifications
*/

WITH planet_base AS (
    SELECT
        p.planet_id,
        p.name,
        p.rotation_period,
        p.orbital_period,
        p.diameter,
        p.climate,
        p.gravity,
        p.terrain,
        p.surface_water,
        p.population
    FROM "nerd_facts"."public"."int_swapi_planets" p
),

-- Film appearances
film_appearances AS (
    SELECT
        fp.planet_id,
        COUNT(DISTINCT fp.film_id) AS film_count,
        STRING_AGG(f.title, ', ' ORDER BY f.episode_id) AS film_appearances
    FROM "nerd_facts"."public"."int_swapi_films_planets" fp
    JOIN "nerd_facts"."public"."int_swapi_films" f ON fp.film_id = f.film_id
    GROUP BY fp.planet_id
),

-- Character counts (residents)
character_counts AS (
    SELECT
        pc.planet_id,
        COUNT(DISTINCT pc.people_id) AS character_count
    FROM "nerd_facts"."public"."int_swapi_planets_characters" pc
    GROUP BY pc.planet_id
),

-- Calculate additional metrics and classifications
planet_enriched AS (
    SELECT
        pb.*,
        COALESCE(fa.film_count, 0) AS film_count,
        COALESCE(cc.character_count, 0) AS character_count,
        COALESCE(fa.film_appearances, 'None') AS film_appearances,
        
        -- Planet size classification
        CASE
            WHEN pb.diameter::NUMERIC > 15000 THEN 'Very Large'
            WHEN pb.diameter::NUMERIC > 10000 THEN 'Large'
            WHEN pb.diameter::NUMERIC > 5000 THEN 'Medium'
            WHEN pb.diameter::NUMERIC > 0 THEN 'Small'
            ELSE 'Unknown'
        END AS size_classification,
        
        -- Population density (people per square km)
        -- Surface area = 4 * pi * r^2, r = diameter/2
        CASE 
            WHEN pb.diameter::NUMERIC > 0 AND pb.population::NUMERIC > 0 
            THEN pb.population::NUMERIC / (4 * 3.14159 * POWER(pb.diameter::NUMERIC/2, 2))
            ELSE NULL
        END AS population_density,
        
        -- Climate type classification
        CASE
            WHEN pb.climate ILIKE '%temperate%' THEN 'Temperate'
            WHEN pb.climate ILIKE '%tropical%' THEN 'Tropical'
            WHEN pb.climate ILIKE '%arid%' OR pb.climate ILIKE '%desert%' THEN 'Arid'
            WHEN pb.climate ILIKE '%frozen%' OR pb.climate ILIKE '%ice%' OR pb.climate ILIKE '%frigid%' THEN 'Frozen'
            WHEN pb.climate ILIKE '%humid%' OR pb.climate ILIKE '%moist%' THEN 'Humid'
            WHEN pb.climate ILIKE '%murky%' OR pb.climate ILIKE '%swamp%' THEN 'Swampy'
            ELSE 'Other'
        END AS climate_classification,
        
        -- Primary terrain classification
        CASE
            WHEN pb.terrain ILIKE '%mountain%' THEN 'Mountainous'
            WHEN pb.terrain ILIKE '%jungle%' THEN 'Jungle'
            WHEN pb.terrain ILIKE '%desert%' THEN 'Desert'
            WHEN pb.terrain ILIKE '%forest%' THEN 'Forested'
            WHEN pb.terrain ILIKE '%ocean%' OR pb.terrain ILIKE '%sea%' THEN 'Oceanic'
            WHEN pb.terrain ILIKE '%swamp%' THEN 'Swamp'
            WHEN pb.terrain ILIKE '%city%' OR pb.terrain ILIKE '%urban%' THEN 'Urban'
            ELSE 'Mixed'
        END AS primary_terrain_classification
    FROM planet_base pb
    LEFT JOIN film_appearances fa ON pb.planet_id = fa.planet_id
    LEFT JOIN character_counts cc ON pb.planet_id = cc.planet_id
)

SELECT
    -- Primary Key
    md5(cast(coalesce(cast(pe.planet_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS planet_key,
    
    -- Natural Key
    pe.planet_id,
    
    -- Planet Attributes
    pe.name AS planet_name,
    pe.rotation_period,
    pe.orbital_period,
    pe.diameter,
    pe.climate,
    pe.gravity,
    pe.terrain,
    pe.surface_water,
    pe.population,
    
    -- Planet Classifications
    pe.size_classification,
    pe.climate_classification,
    pe.primary_terrain_classification,
    
    -- Derived Metrics
    pe.population_density,
    pe.film_count,
    pe.character_count,
    pe.film_appearances,
    
    -- Habitability metrics
    CASE 
        WHEN pe.climate ILIKE '%temperate%' AND pe.surface_water::NUMERIC > 0 THEN 'High'
        WHEN pe.climate NOT ILIKE '%frozen%' AND pe.climate NOT ILIKE '%arid%' THEN 'Medium'
        ELSE 'Low'
    END AS habitability,
    
    -- Is Core World
    CASE 
        WHEN pe.name IN ('Coruscant', 'Alderaan', 'Corellia', 'Chandrila', 'Hosnian Prime') THEN TRUE
        ELSE FALSE
    END AS is_core_world,
    
    -- Notable planets
    CASE 
        WHEN pe.name IN ('Coruscant', 'Tatooine', 'Naboo', 'Hoth', 'Endor', 'Dagobah', 
                        'Bespin', 'Mustafar', 'Death Star', 'Jakku', 'Ahch-To', 'Exegol') 
            OR pe.film_count >= 3
        THEN TRUE
        ELSE FALSE
    END AS is_notable_planet,
    
    -- Narrative importance
    CASE
        WHEN pe.film_count >= 3 THEN 'Major'
        WHEN pe.film_count >= 2 THEN 'Significant'
        WHEN pe.film_count = 1 THEN 'Featured'
        ELSE 'Minor'
    END AS narrative_importance,
    
    -- Data Tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM planet_enriched pe