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
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Id fields
        id,
        episode_id,

        -- Text fields
        title,        
        opening_crawl,
        director,
        producer,
        url,

        -- Date fields
        release_date,
        created,
        edited,

        -- Relationship arrays
        planets,
        starships,
        vehicles,
        species,
        characters                        
    FROM {{ source('swapi', 'films') }}
    WHERE id IS NOT NULL
)

SELECT
        -- Id fields
        id as film_id,
        CAST(episode_id AS INTEGER) AS episode_id,

        -- Text fields
        title,        
        opening_crawl,
        director,
        producer,
        url,

        -- Date fields
        CAST(release_date as DATE) AS release_date,

        -- Timestamp fields
        CAST(created as TIMESTAMP) AS created_at,
        CAST(edited AS TIMESTAMP) AS edited_at,

        -- Relationship arrays
        planets,
        starships,
        vehicles,
        species,
        characters,

        -- Add data tracking field
        CURRENT_TIMESTAMP AS dbt_loaded_at    
FROM raw_data
