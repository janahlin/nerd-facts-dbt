

/*
  Model: stg_swapi_planets
  Description: Standardizes Star Wars planet data from SWAPI
  Source: raw.swapi_planets
*/

WITH raw_data AS (
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
        url,
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_planets"
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS planet_name,
    
    -- Physical characteristics with proper numeric handling
    CAST(NULLIF(rotation_period, 'unknown') AS NUMERIC) AS rotation_period,
    CAST(NULLIF(orbital_period, 'unknown') AS NUMERIC) AS orbital_period,
    CAST(NULLIF(diameter, 'unknown') AS NUMERIC) AS diameter,
    climate,
    gravity,
    terrain,
    CAST(NULLIF(surface_water, 'unknown') AS NUMERIC) AS surface_water,
    CAST(NULLIF(REPLACE(population, ',', ''), 'unknown') AS NUMERIC) AS population,
    
    -- Entity relationships - we'll populate these later
    0 AS resident_count,
    0 AS film_appearances,
    
    -- Placeholders for raw arrays
    NULL::jsonb AS residents,
    NULL::jsonb AS films,
    
    -- Placeholders for name arrays
    NULL AS resident_names,
    NULL AS film_names,
    
    -- Terrain classification flags
    terrain LIKE '%temperate%' AS is_temperate,
    terrain LIKE '%forest%' OR terrain LIKE '%jungle%' OR terrain LIKE '%grassland%' AS has_vegetation,
    terrain LIKE '%ocean%' OR terrain LIKE '%lake%' OR surface_water = '100' AS is_water_world,
    terrain LIKE '%desert%' AS is_desert_world,
    
    -- Source URL 
    url,
    
    -- ETL tracking fields
    NULL::TIMESTAMP AS fetch_timestamp,
    NULL::TIMESTAMP AS processed_timestamp,
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data