

/*
  Model: stg_swapi_people
  Description: Standardizes Star Wars character data from SWAPI
  Source: raw.swapi_people
  
  Notes:
  - Physical attributes are cleaned and converted to proper numeric types
  - Additional derived fields are added for character analysis
  - NOTE: This table doesn't have direct species linkage - this will need to be
    joined from the raw.swapi_species table which references people
*/

WITH raw_data AS (
    SELECT
        id,
        name,
        height,
        mass,
        hair_color,
        skin_color,
        eye_color,
        birth_year,
        gender,
        homeworld,
        url,
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_people"
    WHERE id IS NOT NULL
)

SELECT
    id,
    name,
    NULL AS species_id,
    homeworld AS homeworld_id,
    
    -- Convert height to numeric
    CASE 
        WHEN height IS NULL OR lower(height) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(height, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS height_cm,
    
    -- Convert mass to numeric
    CASE 
        WHEN mass IS NULL OR lower(mass) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(mass, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS mass_kg,
    
    -- Character attributes
    LOWER(COALESCE(hair_color, 'unknown')) AS hair_color,
    LOWER(COALESCE(skin_color, 'unknown')) AS skin_color,
    LOWER(COALESCE(eye_color, 'unknown')) AS eye_color,
    birth_year,
    LOWER(COALESCE(gender, 'unknown')) AS gender,
    
    -- Force detection
    CASE
        WHEN LOWER(name) IN ('luke skywalker', 'darth vader', 'obi-wan kenobi', 'yoda', 
                           'emperor palpatine', 'count dooku', 'qui-gon jinn', 'mace windu',
                           'rey', 'kylo ren', 'anakin skywalker', 'leia organa', 
                           'ahsoka tano', 'darth maul')
        THEN TRUE
        ELSE FALSE
    END AS force_sensitive,
    
    -- Placeholders for relationship fields
    0 AS starship_count,
    0 AS vehicle_count,
    0 AS film_appearances,
    NULL::jsonb AS vehicles,
    NULL::jsonb AS starships,
    NULL::jsonb AS films,
    NULL AS film_names,
    NULL AS starship_names,
    NULL AS vehicle_names,
    NULL AS character_era,
    
    -- Metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    url,
    NULL::TIMESTAMP AS fetch_timestamp,
    NULL::TIMESTAMP AS processed_timestamp,
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data