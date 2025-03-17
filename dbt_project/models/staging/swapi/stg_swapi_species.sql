{{
  config(
    materialized = 'view'
  )
}}

/*
  Model: stg_swapi_species
  Description: Standardizes Star Wars species data from SWAPI
  Source: raw.swapi_species
*/

WITH raw_data AS (
    SELECT
        id,
        name,
        classification,
        designation,
        average_height,
        skin_colors,
        hair_colors,
        eye_colors,
        average_lifespan,
        homeworld,
        language,
        url,
        created,
        edited
    FROM {{ source('swapi', 'species') }}
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS species_name,
    
    -- Species attributes with proper handling
    classification,
    designation,
    CAST(NULLIF(average_height, 'unknown') AS NUMERIC) AS average_height,
    average_lifespan,
    skin_colors,
    hair_colors,
    eye_colors,
    language,
    CAST(NULLIF(homeworld, 'null') AS INTEGER) AS homeworld_id,
    
    -- Entity relationships with placeholder counts
    0 AS people_count,
    0 AS film_appearances,
    
    -- Placeholders for arrays
    NULL::jsonb AS people,
    NULL::jsonb AS films,
    
    -- Placeholders for name arrays
    NULL AS character_names,
    NULL AS film_names,
    
    -- Source URL
    url,
    
    -- ETL tracking fields
    NULL::TIMESTAMP AS fetch_timestamp,
    NULL::TIMESTAMP AS processed_timestamp,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
