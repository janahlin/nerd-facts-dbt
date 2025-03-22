

/*
  Model: stg_swapi_people
  Description: Standardizes Star Wars character data from SWAPI
  Source: raw.swapi_people
  
  Notes:
  - Physical attributes are cleaned and converted to proper numeric types  
*/

WITH raw_data AS (
    SELECT
        -- Id fields
        id,

        -- Text fields
        name,
        
        hair_color,
        skin_color,
        eye_color,
        birth_year,
        gender,
        homeworld,
        url,

        -- Numeric fields
        CASE WHEN height~E'^[0-9]+$' THEN height ELSE NULL END AS height,
        CASE WHEN mass~E'^[0-9]+$' THEN mass ELSE NULL END AS mass,

        -- Timestamp fields
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_people"
    WHERE id IS NOT NULL
)
    select
        -- Id fields
        id as people_id,

        -- Text fields
        name,
        
        hair_color,
        skin_color,
        eye_color,
        birth_year,
        gender,
        homeworld,
        url,

        -- Numeric fields
        CAST(height AS NUMERIC) AS height,
        CAST(mass AS NUMERIC) AS mass,

        -- Timestamp fields
        CAST(created AS TIMESTAMP) AS created_at,
        CAST(edited AS TIMESTAMP) AS edited_at,
        
        -- Add data tracking fields
        CURRENT_TIMESTAMP AS dbt_loaded_at
        FROM raw_data