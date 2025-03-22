
  create view "nerd_facts"."public"."stg_swapi_planets__dbt_tmp"
    
    
  as (
    

/*
  Model: stg_swapi_planets
  Description: Standardizes Star Wars planet data from SWAPI
  Source: raw.swapi_planets
*/

WITH raw_data AS (
    SELECT
        -- Identitfiers
        id,

        -- Text fields
        name,        
        climate,
        gravity,
        terrain,
        url,

        -- Numeric fields
        CASE WHEN surface_water~E'^[0-9]+$' THEN surface_water ELSE NULL END AS surface_water,
        CASE WHEN rotation_period~E'^[0-9]+$' THEN rotation_period ELSE NULL END AS rotation_period,
        CASE WHEN orbital_period~E'^[0-9]+$' THEN orbital_period ELSE NULL END AS orbital_period,
        CASE WHEN diameter~E'^[0-9]+$' THEN diameter ELSE NULL END AS diameter,
        CASE WHEN population~E'^[0-9]+$' THEN population ELSE NULL END AS population,
        
        -- Timestamp fields
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_planets"
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id as planet_id,
    name AS planet_name,

    -- Text fields
    name,        
    climate,
    gravity,
    terrain,
    url,
    
    -- Physical characteristics with proper numeric handling
    CAST(surface_water AS NUMERIC) AS surface_water,
    CAST(rotation_period AS NUMERIC) AS rotation_period,
    CAST(orbital_period AS NUMERIC) AS orbital_period,
    CAST(diameter AS NUMERIC) AS diameter,        
    CAST(population AS NUMERIC) AS population,    
    
    -- ETL tracking fields
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS updated_at,    
    CURRENT_TIMESTAMP AS dbt_loaded_at    
FROM raw_data
  );