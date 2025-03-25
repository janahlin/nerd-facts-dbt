
  
    

  create  table "nerd_facts"."public"."fact_sw_characters__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: fact_sw_characters
  Description: Fact table for Star Wars characters with attributes and metrics
*/

-- Get just the base data first
WITH characters AS (
    SELECT
        people_id AS character_id,
        name,
        height AS height_cm,
        mass AS mass_kg,
        hair_color,
        skin_color,
        eye_color,
        birth_year,
        gender,
        homeworld AS homeworld_id,
        -- These fields don't exist in staging, we'll need to handle them differently
        NULL AS species_id,
        NULL AS film_appearances,
        NULL AS film_names,
        NULL AS vehicle_count,
        NULL AS vehicle_names,
        NULL AS starship_count,
        NULL AS starship_names,
        FALSE AS force_sensitive,
        'Unknown' AS character_era,
        url,
        created_at AS fetch_timestamp,
        edited_at AS processed_timestamp
    FROM "nerd_facts"."public"."stg_swapi_people"
    WHERE people_id IS NOT NULL
)

-- Simplified final output
SELECT
    md5(cast(coalesce(cast(character_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
    character_id,
    name AS character_name,
    homeworld_id,
    species_id,
    
    -- Physical attributes
    height_cm,
    mass_kg,
    hair_color,
    eye_color,
    skin_color,
    birth_year,
    gender,
    
    -- Film and vehicle appearances
    film_appearances,
    vehicle_count,
    starship_count,
    film_names AS film_list,
    vehicle_names AS vehicle_list,
    starship_names AS starship_list,
    
    -- Force user information
    force_sensitive,
    
    -- Era classification
    character_era,
    
    -- Affiliation (simplified)
    CASE
        WHEN LOWER(name) IN ('luke skywalker', 'leia organa', 'han solo') THEN 'Rebel Alliance'
        WHEN LOWER(name) IN ('darth vader', 'emperor palpatine') THEN 'Empire'
        ELSE 'Other'
    END AS affiliation,
    
    -- Character tier (simplified)
    CASE
        WHEN LOWER(name) IN ('luke skywalker', 'darth vader', 'leia organa') THEN 'S'
        WHEN COALESCE(film_appearances::integer, 0) > 2 THEN 'A'
        WHEN COALESCE(film_appearances::integer, 0) > 1 THEN 'B' 
        ELSE 'C'
    END AS character_tier,
    
    -- Source tracking
    url,
    fetch_timestamp,
    processed_timestamp,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM characters
ORDER BY character_id
  );
  