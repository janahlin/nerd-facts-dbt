/*
  Model: stg_swapi_people
  Description: Standardizes Star Wars character data from SWAPI
  Source: raw.swapi_people
  
  Notes:
  - Species and homeworld are extracted from JSON references
  - Force sensitivity is derived from character appearances and names
  - Physical attributes are cleaned and converted to proper numeric types
  - Additional derived fields are added for character analysis
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
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
        films,
        species,
        vehicles,
        starships,
        created,
        edited,
        url
    FROM raw.swapi_people
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name,
    
    -- Extract species information with improved error handling
    CASE 
        WHEN species IS NULL OR jsonb_array_length(species) = 0 THEN 'Human'  -- Default to Human if no species
        WHEN jsonb_typeof(species) != 'array' THEN 'Unknown'  -- Handle unexpected JSON type
        ELSE COALESCE(SPLIT_PART(species->0->>'url', '/', 6), 'Unknown')  -- Extract ID from URL
    END AS species,
    
    -- Extract homeworld ID with error handling
    CASE
        WHEN homeworld IS NULL THEN NULL
        WHEN jsonb_typeof(homeworld) != 'object' THEN NULL
        ELSE NULLIF(SPLIT_PART(homeworld->>'url', '/', 6), '')
    END AS homeworld_id,
    
    -- Clean numeric values with comprehensive error handling
    CASE 
        WHEN height IS NULL OR lower(height) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(height, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS height_cm,
    
    CASE 
        WHEN mass IS NULL OR lower(mass) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(mass, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS mass_kg,
    
    -- Calculate BMI if both height and mass are available
    CASE
        WHEN height IS NOT NULL AND mass IS NOT NULL 
             AND NULLIF(REGEXP_REPLACE(height, '[^0-9\.]', '', 'g'), '') != ''
             AND NULLIF(REGEXP_REPLACE(mass, '[^0-9\.]', '', 'g'), '') != ''
             AND height::NUMERIC > 0
        THEN (NULLIF(REGEXP_REPLACE(mass, '[^0-9\.]', '', 'g'), '')::NUMERIC) / 
             POWER((NULLIF(REGEXP_REPLACE(height, '[^0-9\.]', '', 'g'), '')::NUMERIC / 100), 2)
        ELSE NULL
    END AS bmi,
    
    -- Character physical attributes with standardized colors
    LOWER(COALESCE(hair_color, 'unknown')) AS hair_color,
    LOWER(COALESCE(skin_color, 'unknown')) AS skin_color,
    LOWER(COALESCE(eye_color, 'unknown')) AS eye_color,
    birth_year,
    LOWER(COALESCE(gender, 'unknown')) AS gender,
    
    -- Force-user related fields with expanded detection
    CASE
        -- Check film appearances
        WHEN films @> '[{"title": "Return of the Jedi"}]' 
             OR films @> '[{"title": "The Empire Strikes Back"}]'
             OR films @> '[{"title": "Revenge of the Sith"}]'
        -- Explicit Jedi/Sith list
        OR LOWER(name) IN ('luke skywalker', 'darth vader', 'obi-wan kenobi', 'yoda', 
                          'emperor palpatine', 'count dooku', 'qui-gon jinn', 'mace windu',
                          'rey', 'kylo ren')
        THEN TRUE
        ELSE FALSE
    END AS force_sensitive,
    
    -- Force rating for known powerful force users
    CASE
        WHEN LOWER(name) IN ('yoda', 'emperor palpatine', 'darth vader', 'luke skywalker') THEN 5
        WHEN LOWER(name) IN ('obi-wan kenobi', 'mace windu', 'kylo ren', 'rey') THEN 4
        WHEN LOWER(name) IN ('qui-gon jinn', 'count dooku') THEN 3
        WHEN force_sensitive THEN 2
        ELSE NULL
    END AS force_rating,
    
    -- Entity counts with error handling
    COALESCE(jsonb_array_length(starships), 0) AS ships_piloted,
    COALESCE(jsonb_array_length(vehicles), 0) AS vehicles_operated,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage
    films,
    starships,
    vehicles,
    
    -- Era classification
    CASE
        WHEN films @> '[{"title": "The Phantom Menace"}]' 
             OR films @> '[{"title": "Attack of the Clones"}]'
             OR films @> '[{"title": "Revenge of the Sith"}]'
        THEN 'Prequel Era'
        WHEN films @> '[{"title": "A New Hope"}]'
             OR films @> '[{"title": "The Empire Strikes Back"}]'
             OR films @> '[{"title": "Return of the Jedi"}]'
        THEN 'Original Trilogy Era'
        WHEN films @> '[{"title": "The Force Awakens"}]'
             OR films @> '[{"title": "The Last Jedi"}]'
        THEN 'Sequel Era'
        ELSE 'Unknown Era'
    END AS character_era,
    
    -- API metadata with proper handling
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data