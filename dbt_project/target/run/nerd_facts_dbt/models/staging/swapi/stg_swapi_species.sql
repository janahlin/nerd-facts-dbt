
  create view "nerd_facts"."public"."stg_swapi_species__dbt_tmp"
    
    
  as (
    

/*
  Model: stg_swapi_species
  Description: Standardizes Star Wars species data from SWAPI
  Source: raw.swapi_species
  
  Notes:
  - Numeric fields are cleaned and converted to proper types
  - Color fields are parsed for analysis
  - Homeworld references are extracted
  - Additional derived fields help with species classification
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        classification,
        designation,
        average_height,
        average_lifespan,
        eye_colors,
        hair_colors,
        skin_colors,
        homeworld,
        language,
        CASE WHEN people IS NULL OR people = '' THEN NULL::jsonb ELSE people::jsonb END AS people,
        CASE WHEN films IS NULL OR films = '' THEN NULL::jsonb ELSE films::jsonb END AS films,
        created,
        edited,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM "nerd_facts"."raw"."swapi_species"
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS species_name,
    
    -- Species classification
    LOWER(COALESCE(classification, 'unknown')) AS classification,
    LOWER(COALESCE(designation, 'unknown')) AS designation,
    
    -- Physical characteristics with proper typing
    CASE 
        WHEN average_height IS NULL OR LOWER(average_height) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(average_height, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS average_height_cm,
    
    CASE 
        WHEN average_lifespan IS NULL OR LOWER(average_lifespan) = 'unknown' 
            OR LOWER(average_lifespan) = 'indefinite' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(average_lifespan, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS average_lifespan_years,
    
    -- Special case for indefinite lifespan
    CASE 
        WHEN LOWER(average_lifespan) = 'indefinite' THEN TRUE
        ELSE FALSE
    END AS has_indefinite_lifespan,
    
    -- Language
    LOWER(COALESCE(language, 'unknown')) AS language,
    
    -- Color attributes - standardized to arrays
    CASE 
        WHEN eye_colors IS NULL OR eye_colors = '' THEN NULL
        ELSE STRING_TO_ARRAY(LOWER(eye_colors), ', ') 
    END AS eye_colors_array,
    
    CASE 
        WHEN hair_colors IS NULL OR hair_colors = '' THEN NULL
        ELSE STRING_TO_ARRAY(LOWER(hair_colors), ', ') 
    END AS hair_colors_array,
    
    CASE 
        WHEN skin_colors IS NULL OR skin_colors = '' THEN NULL
        ELSE STRING_TO_ARRAY(LOWER(skin_colors), ', ') 
    END AS skin_colors_array,
    
    -- Extract homeworld with error handling
    CASE
        WHEN homeworld IS NULL THEN NULL
        WHEN homeworld = 'null' THEN NULL
        ELSE NULLIF(SPLIT_PART(homeworld->>'url', '/', 6), '')
    END AS homeworld_id,
    
    -- Entity counts
    COALESCE(jsonb_array_length(people), 0) AS character_count,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage
    people,
    films,
    
    -- Derived species classifications
    CASE
        WHEN LOWER(classification) IN ('mammal', 'amphibian', 'reptile', 'bird') THEN TRUE
        ELSE FALSE
    END AS is_organic,
    
    CASE
        WHEN LOWER(classification) = 'artificial' OR LOWER(name) = 'droid' THEN TRUE
        ELSE FALSE
    END AS is_artificial,
    
    -- Intelligence estimation (approximate)
    CASE
        WHEN LOWER(designation) = 'sentient' THEN 'High'
        WHEN LOWER(designation) = 'semi-sentient' THEN 'Moderate'
        ELSE 'Unknown'
    END AS intelligence_level,
    
    -- Longevity classification
    CASE
        WHEN LOWER(average_lifespan) = 'indefinite' THEN 'Immortal'
        WHEN average_lifespan::NUMERIC > 500 THEN 'Very Long-Lived'
        WHEN average_lifespan::NUMERIC > 100 THEN 'Long-Lived'
        WHEN average_lifespan::NUMERIC > 70 THEN 'Standard'
        WHEN average_lifespan::NUMERIC > 0 THEN 'Short-Lived'
        ELSE 'Unknown'
    END AS longevity_class,
    
    -- Notable species flag
    CASE
        WHEN name IN ('Human', 'Wookiee', 'Droid', 'Hutt', 'Ewok', 'Gungan', 
                     'Jawa', 'Mon Calamari', 'Twi\'lek', 'Yoda\'s species') 
        THEN TRUE
        ELSE FALSE
    END AS is_notable_species,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
  );