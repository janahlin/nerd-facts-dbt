{{
  config(
    materialized = 'view'
  )
}}

/*
  Model: stg_swapi_planets
  Description: Standardizes Star Wars planet data from SWAPI
  Source: raw.swapi_planets
  
  Notes:
  - Numeric fields are cleaned and converted to proper types
  - Population is handled with appropriate NULL values
  - Derived planet classifications are added for analysis
  - Climate and terrain are standardized for consistency
*/

-- First check what columns actually exist
WITH column_check AS (
    SELECT 
        column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'raw' 
    AND table_name = 'swapi_planets'
),

raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        rotation_period,
        orbital_period,
        diameter,
        climate,
        gravity,
        terrain,
        surface_water,
        population,
        
        -- Handle various possible column names for resident data
        CASE
            -- Check for standard "residents" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'residents')
            THEN CASE WHEN residents IS NULL OR residents = '' THEN NULL::jsonb ELSE residents::jsonb END
            
            -- Check for alternative "people" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'people')
            THEN CASE WHEN people IS NULL OR people = '' THEN NULL::jsonb ELSE people::jsonb END
            
            -- Check for alternative "characters" column
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'characters')
            THEN CASE WHEN characters IS NULL OR characters = '' THEN NULL::jsonb ELSE characters::jsonb END
            
            -- Default to empty array if no matching column found
            ELSE '[]'::jsonb
        END AS residents,
        
        -- Handle films data with similar approach
        CASE
            WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'films')
            THEN CASE WHEN films IS NULL OR films = '' THEN NULL::jsonb ELSE films::jsonb END
            
            -- Default to empty array if no matching column found
            ELSE '[]'::jsonb
        END AS films,
        
        -- Check for name arrays
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'resident_names') 
             THEN resident_names ELSE NULL END AS resident_names,

        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'film_names') 
             THEN film_names ELSE NULL END AS film_names,

        -- Source URL
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'url') 
             THEN url ELSE NULL END AS url,

        -- ETL tracking fields
        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'fetch_timestamp') 
             THEN fetch_timestamp ELSE NULL END AS fetch_timestamp,

        CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'processed_timestamp') 
             THEN processed_timestamp ELSE NULL END AS processed_timestamp,

        created,
        edited,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM {{ source('swapi', 'planets') }}
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS planet_name,
    
    -- Physical characteristics with proper typing
    CASE 
        WHEN rotation_period IS NULL OR lower(rotation_period) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(rotation_period, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS rotation_period,
    
    CASE 
        WHEN orbital_period IS NULL OR lower(orbital_period) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(orbital_period, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS orbital_period,
    
    CASE 
        WHEN diameter IS NULL OR lower(diameter) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(diameter, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS diameter,
    
    -- Environment attributes
    LOWER(COALESCE(climate, 'unknown')) AS climate,
    LOWER(COALESCE(gravity, 'unknown')) AS gravity,
    LOWER(COALESCE(terrain, 'unknown')) AS terrain,
    
    CASE 
        WHEN surface_water IS NULL OR lower(surface_water) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(surface_water, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS surface_water,
    
    -- Population with proper handling
    CASE 
        WHEN population IS NULL OR lower(population) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(population, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS population,
    
    -- Entity counts
    COALESCE(jsonb_array_length(residents), 0) AS resident_count,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage
    residents,
    films,
    resident_names,
    film_names,
    
    -- Derived classifications
    CASE
        WHEN lower(climate) LIKE '%temperate%' THEN TRUE
        ELSE FALSE
    END AS is_temperate,
    
    CASE
        WHEN lower(terrain) LIKE '%forest%' OR 
             lower(terrain) LIKE '%jungle%' OR
             lower(terrain) LIKE '%grasslands%' THEN TRUE
        ELSE FALSE
    END AS has_vegetation,
    
    CASE
        WHEN lower(terrain) LIKE '%ocean%' OR 
             lower(terrain) LIKE '%water%' OR
             (surface_water IS NOT NULL AND 
              CASE WHEN surface_water ~ '^[0-9\.]+$' 
                   THEN surface_water::NUMERIC > 50 
                   ELSE FALSE 
              END) THEN TRUE
        ELSE FALSE
    END AS is_water_world,
    
    CASE
        WHEN lower(terrain) LIKE '%desert%' OR 
             lower(climate) LIKE '%arid%' OR
             (surface_water IS NOT NULL AND 
              CASE WHEN surface_water ~ '^[0-9\.]+$' 
                   THEN surface_water::NUMERIC < 5 
                   ELSE FALSE 
              END) THEN TRUE
        ELSE FALSE
    END AS is_desert_world,
    
    -- Planet habitability score (0-100)
    CASE
        WHEN lower(climate) LIKE '%temperate%' THEN
            CASE
                WHEN lower(terrain) LIKE '%forest%' OR 
                     lower(terrain) LIKE '%grasslands%' THEN 90
                ELSE 75
            END
        WHEN lower(climate) LIKE '%tropical%' OR lower(climate) LIKE '%humid%' THEN 65
        WHEN lower(climate) LIKE '%arid%' OR lower(climate) LIKE '%hot%' THEN 35
        WHEN lower(climate) LIKE '%frozen%' OR lower(climate) LIKE '%frigid%' THEN 15
        ELSE 50
    END AS habitability_score,
    
    -- Notable planet flag
    CASE
        WHEN name IN ('Tatooine', 'Alderaan', 'Yavin IV', 'Hoth', 'Dagobah', 
                      'Bespin', 'Endor', 'Naboo', 'Coruscant', 'Kamino', 'Geonosis') 
        THEN TRUE
        ELSE FALSE
    END AS is_notable_planet,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at,
    
    -- ETL tracking fields
    CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'fetch_timestamp') 
         THEN fetch_timestamp ELSE NULL END AS fetch_timestamp,

    CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'processed_timestamp') 
         THEN processed_timestamp ELSE NULL END AS processed_timestamp,

    -- Source URL
    CASE WHEN EXISTS (SELECT 1 FROM column_check WHERE column_name = 'url') 
         THEN url ELSE NULL END AS url,
    
    -- Final SELECT should include:
    -- Source URL
    url,
    
    -- Name arrays for easier reporting
    resident_names,
    film_names,
    
    -- ETL tracking fields
    fetch_timestamp::TIMESTAMP AS fetch_timestamp,
    processed_timestamp::TIMESTAMP AS processed_timestamp
    
FROM raw_data
