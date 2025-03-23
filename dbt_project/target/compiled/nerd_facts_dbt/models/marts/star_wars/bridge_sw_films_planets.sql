

/*
  Model: bridge_sw_films_planets
  Description: Bridge table connecting films to planets with enriched relationship attributes
*/

WITH film_planets AS (
    SELECT
        fp.film_id,
        fp.planet_id,
        f.title AS film_title,
        p.name AS planet_name,
        f.release_date,
        f.episode_id,
        p.climate,
        p.terrain
    FROM "nerd_facts"."public"."int_swapi_films_planets" fp
    JOIN "nerd_facts"."public"."stg_swapi_films" f ON fp.film_id = f.film_id
    JOIN "nerd_facts"."public"."stg_swapi_planets" p ON fp.planet_id = p.planet_id
),

-- Add trilogy and appearance information
film_planets_enriched AS (
    SELECT 
        fp.*,
        -- Trilogy classification
        CASE
            WHEN fp.episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
            WHEN fp.episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy'
            WHEN fp.episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
            ELSE 'Anthology Films'
        END AS trilogy,
        
        -- Extract year for easier querying
        EXTRACT(YEAR FROM fp.release_date) AS release_year,
        
        -- Planet appearance metrics
        COUNT(*) OVER (PARTITION BY fp.planet_id) AS planet_film_count,
        (SELECT COUNT(DISTINCT film_id) FROM "nerd_facts"."public"."stg_swapi_films") AS total_films,
        
        -- Order of appearance
        ROW_NUMBER() OVER (
            PARTITION BY fp.planet_id 
            ORDER BY COALESCE(fp.episode_id, 999)
        ) AS chronological_appearance_number,
        
        ROW_NUMBER() OVER (
            PARTITION BY fp.planet_id 
            ORDER BY fp.release_date
        ) AS release_order_appearance_number
    FROM film_planets fp
)

SELECT
    -- Primary Key
    md5(cast(coalesce(cast(fpe.film_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fpe.planet_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS film_planet_key,
    
    -- Foreign Keys
    md5(cast(coalesce(cast(fpe.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS film_key,
    md5(cast(coalesce(cast(fpe.planet_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS planet_key,
    
    -- Source IDs
    fpe.film_id,
    fpe.planet_id,
    
    -- Basic Attributes
    fpe.film_title,
    fpe.planet_name,
    fpe.episode_id,
    
    -- Film categorization
    fpe.trilogy AS film_saga,
    fpe.release_year,
    
    -- Planet characteristics
    fpe.climate,
    fpe.terrain,
    
    -- Appearance metrics
    fpe.chronological_appearance_number,
    fpe.release_order_appearance_number,
    
    -- Planet appearance percentage across all films
    (fpe.planet_film_count::FLOAT / NULLIF(fpe.total_films, 0)::FLOAT) * 100 AS saga_appearance_percentage,
    
    -- Timeline attributes
    CASE WHEN fpe.chronological_appearance_number = 1 THEN TRUE ELSE FALSE END AS is_first_chronological_appearance,
    CASE WHEN fpe.release_order_appearance_number = 1 THEN TRUE ELSE FALSE END AS is_first_release_appearance,
    
    -- Planet significance in the film
    CASE
        WHEN fpe.planet_name IN ('Tatooine', 'Coruscant', 'Naboo', 'Death Star', 'Hoth', 
                                'Dagobah', 'Endor', 'Bespin', 'Jakku', 'Starkiller Base', 
                                'Exegol', 'Scarif', 'Mustafar') 
            THEN 'Major Setting'
        ELSE 'Minor Setting'
    END AS planet_significance,
    
    -- Scene type (based on climate and terrain)
    CASE
        WHEN fpe.climate ILIKE '%arid%' OR fpe.climate ILIKE '%desert%' OR fpe.terrain ILIKE '%desert%'
            THEN 'Desert Scene'
        WHEN fpe.climate ILIKE '%frozen%' OR fpe.climate ILIKE '%ice%' OR fpe.climate ILIKE '%frigid%'
            THEN 'Ice Scene'
        WHEN fpe.terrain ILIKE '%forest%' OR fpe.terrain ILIKE '%jungle%' OR fpe.terrain ILIKE '%rain%'
            THEN 'Forest Scene'
        WHEN fpe.terrain ILIKE '%city%' OR fpe.terrain ILIKE '%urban%'
            THEN 'Urban Scene'
        WHEN fpe.climate ILIKE '%swamp%' OR fpe.terrain ILIKE '%swamp%'
            THEN 'Swamp Scene'
        WHEN fpe.climate ILIKE '%toxic%' OR fpe.terrain ILIKE '%lava%' OR fpe.terrain ILIKE '%volcanic%'
            THEN 'Hostile Environment Scene'
        ELSE 'Mixed Scene'
    END AS scene_type,
    
    -- Data Tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM film_planets_enriched fpe
ORDER BY 
    COALESCE(fpe.episode_id, 999),
    fpe.planet_name