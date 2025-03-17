{{
  config(
    materialized = 'view'
  )
}}

/*
  Model: stg_swapi_films
  Description: Standardizes Star Wars film data from SWAPI
  Source: raw.swapi_films
  
  Notes:
  - Numeric fields are cleaned and converted to proper types
  - Character and vehicle references are extracted as counts
  - Additional derived fields help with film classification
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        title,
        episode_id,
        opening_crawl,
        director,
        producer,
        release_date,
        
        -- Handle relationship arrays with proper type casting
        CASE WHEN characters IS NULL OR characters = '' THEN NULL::jsonb ELSE characters::jsonb END AS characters,
        CASE WHEN planets IS NULL OR planets = '' THEN NULL::jsonb ELSE planets::jsonb END AS planets,
        CASE WHEN starships IS NULL OR starships = '' THEN NULL::jsonb ELSE starships::jsonb END AS starships,
        CASE WHEN vehicles IS NULL OR vehicles = '' THEN NULL::jsonb ELSE vehicles::jsonb END AS vehicles,
        CASE WHEN species IS NULL OR species = '' THEN NULL::jsonb ELSE species::jsonb END AS species,
        
        -- Source URL and tracking fields
        url,
        created,
        edited
    FROM {{ source('swapi', 'films') }}
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    title AS film_title,
    CAST(episode_id AS INTEGER) AS episode_id,  -- Cast to integer
    
    -- Film metadata
    opening_crawl,
    director,
    producer,
    CAST(release_date AS DATE) AS release_date,
    
    -- Entity counts with error handling
    COALESCE(jsonb_array_length(characters), 0) AS character_count,
    COALESCE(jsonb_array_length(planets), 0) AS planet_count,
    COALESCE(jsonb_array_length(starships), 0) AS starship_count,
    COALESCE(jsonb_array_length(vehicles), 0) AS vehicle_count,
    COALESCE(jsonb_array_length(species), 0) AS species_count,
    
    -- Keep raw arrays for downstream usage
    characters,
    planets,
    starships,
    vehicles,
    species,
    
    -- Create arrays of extracted names for reporting (placeholder for future enhancement)
    NULL AS character_names,
    NULL AS planet_names,
    
    -- Derived film classification (use the casted episode_id field)
    CASE
        WHEN CAST(episode_id AS INTEGER) BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
        WHEN CAST(episode_id AS INTEGER) BETWEEN 4 AND 6 THEN 'Original Trilogy'
        WHEN CAST(episode_id AS INTEGER) BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
        ELSE 'Anthology'
    END AS trilogy,
    
    -- Era classification (use the casted episode_id field)
    CASE
        WHEN CAST(episode_id AS INTEGER) BETWEEN 1 AND 3 THEN 'Republic Era'
        WHEN CAST(episode_id AS INTEGER) BETWEEN 4 AND 6 THEN 'Imperial Era'
        WHEN CAST(episode_id AS INTEGER) BETWEEN 7 AND 9 THEN 'New Republic Era'
        ELSE 'Various'
    END AS era,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Source URL
    url,
    
    -- ETL tracking fields (placeholder for future enhancement)
    NULL::TIMESTAMP AS fetch_timestamp,
    NULL::TIMESTAMP AS processed_timestamp,
    
    -- Add data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
ORDER BY episode_id::INTEGER  -- Cast to integer for ordering
