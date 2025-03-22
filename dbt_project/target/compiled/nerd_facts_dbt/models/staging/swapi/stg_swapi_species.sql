

/*
  Model: stg_swapi_species
  Description: Standardizes Star Wars species data from SWAPI
  Source: raw.swapi_species
*/

WITH raw_data AS (
    SELECT
        -- Id fields
        id,

        -- Text fields
        name,
        classification,
        designation,        
        skin_colors,
        hair_colors,
        eye_colors,                
        language,
        url,

        -- Numeric fields
        CASE average_lifespan~E'^[0-9]+$' WHEN TRUE THEN average_lifespan ELSE NULL END AS average_lifespan,
        CASE average_height~E'^[0-9]+$' WHEN TRUE THEN average_height ELSE NULL END AS average_height,

        -- Relationship arrays
        people,
        homeworld,

        -- Timestamp fields
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_species"
    WHERE id IS NOT NULL
)

SELECT    
    -- Id fields
    id as species_id,

    -- Text fields
    name as species_name,
    classification,
    designation,        
    skin_colors,
    hair_colors,
    eye_colors,                
    language,
    url,

    -- Numeric fields
    CAST(average_lifespan AS NUMERIC) AS average_lifespan,
    CAST(average_height AS NUMERIC) AS average_height,

    -- Relationship arrays
    people,
    homeworld,

    -- Timestamp fields
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS edited_at,

    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at    
FROM raw_data