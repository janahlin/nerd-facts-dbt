{{
  config(
    materialized = 'view'
  )
}}

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

-- First, check what columns actually exist in the source table
WITH column_check AS (
    SELECT 
        column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'raw' 
    AND table_name = 'swapi_people'
),

raw_data AS (
    -- Base selection with only columns we're sure exist
    SELECT
        id,
        name,
        
        -- Physical attributes - always check if they exist first
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'height') 
             THEN height ELSE NULL END AS height,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'mass') 
             THEN mass ELSE NULL END AS mass,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'hair_color') 
             THEN hair_color ELSE NULL END AS hair_color,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'skin_color') 
             THEN skin_color ELSE NULL END AS skin_color,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'eye_color') 
             THEN eye_color ELSE NULL END AS eye_color,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'birth_year') 
             THEN birth_year ELSE NULL END AS birth_year,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'gender') 
             THEN gender ELSE NULL END AS gender,
        
        -- Handle homeworld reference
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'homeworld') 
             THEN homeworld ELSE NULL END AS homeworld,
        
        -- Handle films with multiple possible column names
        CASE 
            -- Check for standard "films" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'films')
            THEN CASE WHEN films IS NULL OR films = '' THEN NULL::jsonb ELSE films::jsonb END
            
            -- Check for alternative "appearances" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'appearances')
            THEN CASE WHEN appearances IS NULL OR appearances = '' THEN NULL::jsonb ELSE appearances::jsonb END
            
            -- Check for alternative "movies" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'movies')
            THEN CASE WHEN movies IS NULL OR movies = '' THEN NULL::jsonb ELSE movies::jsonb END
            
            -- Default to empty array if no matching column found
            ELSE '[]'::jsonb
        END AS films,
        
        -- Handle species reference with multiple possible column names
        CASE 
            -- Check for standard "species" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'species')
            THEN CASE WHEN species IS NULL OR species = '' THEN NULL::jsonb ELSE species::jsonb END
            
            -- Check for alternative "species_id" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'species_id')
            THEN CASE WHEN species_id IS NULL OR species_id = '' 
                      THEN NULL::jsonb ELSE jsonb_build_array(species_id::text) END
            
            -- Default to empty array
            ELSE '[]'::jsonb
        END AS species,
        
        -- Handle vehicles with similar approach
        CASE 
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'vehicles')
            THEN CASE WHEN vehicles IS NULL OR vehicles = '' THEN NULL::jsonb ELSE vehicles::jsonb END
            ELSE '[]'::jsonb
        END AS vehicles,
        
        -- Handle starships with similar approach
        CASE 
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'starships')
            THEN CASE WHEN starships IS NULL OR starships = '' THEN NULL::jsonb ELSE starships::jsonb END
            ELSE '[]'::jsonb
        END AS starships,
        
        -- Metadata fields
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'created') 
             THEN created ELSE NULL END AS created,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'edited') 
             THEN edited ELSE NULL END AS edited,
             
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'url') 
             THEN url ELSE NULL END AS url,
             
        -- Handle film_names array if available
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'film_names') 
             THEN film_names ELSE NULL END AS film_names,
             
        -- ETL tracking fields
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'fetch_timestamp') 
             THEN fetch_timestamp ELSE NULL END AS fetch_timestamp,
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'processed_timestamp') 
             THEN processed_timestamp ELSE NULL END AS processed_timestamp
    FROM {{ source('swapi', 'people') }}
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
        ELSE COALESCE(jsonb_array_elements_text(species), 'Unknown')  -- Extract species ID
    END AS species_id,
    
    -- Extract homeworld ID with error handling
    CASE
        WHEN homeworld IS NULL THEN NULL
        ELSE homeworld::text  -- Just store the raw reference for now
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
             AND (NULLIF(REGEXP_REPLACE(height, '[^0-9\.]', '', 'g'), '')::NUMERIC) > 0
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
    
    -- Force-user detection based on name since we can't check film appearances reliably
    CASE
        WHEN LOWER(name) IN ('luke skywalker', 'darth vader', 'obi-wan kenobi', 'yoda', 
                           'emperor palpatine', 'count dooku', 'qui-gon jinn', 'mace windu',
                           'rey', 'kylo ren', 'anakin skywalker', 'leia organa', 
                           'ahsoka tano', 'darth maul')
        THEN TRUE
        ELSE FALSE
    END AS force_sensitive,
    
    -- Force rating for known powerful force users
    CASE
        WHEN LOWER(name) IN ('yoda', 'emperor palpatine', 'darth vader', 'luke skywalker') THEN 5
        WHEN LOWER(name) IN ('obi-wan kenobi', 'mace windu', 'kylo ren', 'rey') THEN 4
        WHEN LOWER(name) IN ('qui-gon jinn', 'count dooku', 'ahsoka tano') THEN 3
        WHEN LOWER(name) IN ('leia organa', 'darth maul') THEN 2
        WHEN force_sensitive THEN 1
        ELSE NULL
    END AS force_rating,
    
    -- Entity counts with error handling
    COALESCE(jsonb_array_length(starships), 0) AS ships_piloted,
    COALESCE(jsonb_array_length(vehicles), 0) AS vehicles_operated,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage (with safe access)
    films,
    starships,
    vehicles,
    
    -- Era classification based on character name instead of film appearances
    CASE
        WHEN EXISTS (
            SELECT 1 
            FROM jsonb_array_elements_text(films) AS film_id
            JOIN {{ ref('stg_swapi_films') }} f ON f.id::text = film_id
            WHERE f.trilogy = 'Prequel Trilogy'
        ) THEN 'Prequel Era'
        
        WHEN EXISTS (
            SELECT 1 
            FROM jsonb_array_elements_text(films) AS film_id
            JOIN {{ ref('stg_swapi_films') }} f ON f.id::text = film_id
            WHERE f.trilogy = 'Original Trilogy'
        ) THEN 'Original Trilogy Era'
        
        WHEN EXISTS (
            SELECT 1 
            FROM jsonb_array_elements_text(films) AS film_id
            JOIN {{ ref('stg_swapi_films') }} f ON f.id::text = film_id
            WHERE f.trilogy = 'Sequel Trilogy'
        ) THEN 'Sequel Era'
        
        ELSE 'Unknown Era'
    END AS character_era,
    
    -- API metadata with proper handling
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at,
    
    -- Pass through to final SELECT
    url,
    fetch_timestamp::TIMESTAMP AS fetch_timestamp,
    processed_timestamp::TIMESTAMP AS processed_timestamp
    
FROM raw_data