

/*
  Model: stg_swapi_vehicles
  Description: Standardizes Star Wars vehicle data from SWAPI
  Source: raw.swapi_vehicles
*/

WITH raw_data AS (
    SELECT
        id,
        name,
        model,
        manufacturer,
        cost_in_credits,
        length,
        max_atmosphering_speed,
        crew,
        passengers,
        cargo_capacity,
        consumables,
        vehicle_class,
        
        -- Handle relationship arrays with proper type casting
        CASE WHEN pilots IS NULL OR pilots = '' THEN NULL::jsonb ELSE pilots::jsonb END AS pilots,
        CASE WHEN films IS NULL OR films = '' THEN NULL::jsonb ELSE films::jsonb END AS films,
        
        -- Source URL and tracking
        url,
        created,
        edited
    FROM "nerd_facts"."raw"."swapi_vehicles"
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS vehicle_name,
    
    -- Vehicle specifications with proper numeric handling
    model,
    manufacturer,
    CAST(NULLIF(REPLACE(cost_in_credits, ',', ''), 'unknown') AS NUMERIC) AS cost_in_credits,
    CAST(NULLIF(length, 'unknown') AS NUMERIC) AS length_m,
    CAST(NULLIF(REPLACE(max_atmosphering_speed, ',', ''), 'unknown') AS NUMERIC) AS max_speed,
    
    -- Convert crew to numeric with error handling
    CASE
        WHEN crew ~ '^[0-9,.]+$' THEN CAST(REPLACE(crew, ',', '') AS NUMERIC)
        WHEN crew LIKE '%-%' THEN 
            CAST(SPLIT_PART(REPLACE(crew, ',', ''), '-', 1) AS NUMERIC) +
            (CAST(SPLIT_PART(REPLACE(crew, ',', ''), '-', 2) AS NUMERIC) - 
             CAST(SPLIT_PART(REPLACE(crew, ',', ''), '-', 1) AS NUMERIC)) / 2
        ELSE NULL
    END AS crew_count,
    
    -- Convert passengers to numeric with error handling
    CASE
        WHEN passengers ~ '^[0-9,.]+$' THEN CAST(REPLACE(passengers, ',', '') AS NUMERIC)
        WHEN passengers LIKE '%-%' THEN 
            CAST(SPLIT_PART(REPLACE(passengers, ',', ''), '-', 1) AS NUMERIC) +
            (CAST(SPLIT_PART(REPLACE(passengers, ',', ''), '-', 2) AS NUMERIC) - 
             CAST(SPLIT_PART(REPLACE(passengers, ',', ''), '-', 1) AS NUMERIC)) / 2
        ELSE NULL
    END AS passenger_capacity,
    
    CAST(NULLIF(REPLACE(cargo_capacity, ',', ''), 'unknown') AS NUMERIC) AS cargo_capacity,
    consumables,
    vehicle_class,
    
    -- Entity relationships with counts
    COALESCE(jsonb_array_length(pilots), 0) AS pilot_count,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage
    pilots,
    films,
    
    -- Create arrays of extracted names (placeholder for future enhancement)
    NULL AS pilot_names,
    NULL AS film_names,
    
    -- Derived vehicle classifications
    CASE
        WHEN LOWER(vehicle_class) LIKE '%walker%' OR LOWER(vehicle_class) LIKE '%tank%' THEN 'Military'
        WHEN LOWER(vehicle_class) LIKE '%fighter%' OR LOWER(vehicle_class) LIKE '%bomber%' THEN 'Military'
        WHEN LOWER(vehicle_class) LIKE '%transport%' THEN 'Transport'
        WHEN LOWER(vehicle_class) LIKE '%speeder%' THEN 
            CASE
                WHEN LOWER(name) LIKE '%military%' THEN 'Military'
                ELSE 'Civilian'
            END
        WHEN LOWER(vehicle_class) LIKE '%airspeeder%' THEN
            CASE
                WHEN LOWER(name) LIKE '%military%' THEN 'Military'
                ELSE 'Civilian' 
            END
        WHEN LOWER(vehicle_class) LIKE '%shuttle%' THEN 'Transport'
        WHEN LOWER(vehicle_class) LIKE '%barge%' OR LOWER(vehicle_class) LIKE '%yacht%' THEN 'Leisure/Luxury'
        WHEN LOWER(vehicle_class) LIKE '%crawler%' OR LOWER(vehicle_class) LIKE '%digger%' THEN 'Industrial'
        ELSE 'Multipurpose'
    END AS vehicle_purpose,
    
    -- Size classification
    CASE
        WHEN CAST(NULLIF(length, 'unknown') AS NUMERIC) > 100 THEN 'Massive'
        WHEN CAST(NULLIF(length, 'unknown') AS NUMERIC) > 20 THEN 'Large'
        WHEN CAST(NULLIF(length, 'unknown') AS NUMERIC) > 10 THEN 'Medium'
        WHEN CAST(NULLIF(length, 'unknown') AS NUMERIC) > 5 THEN 'Small'
        ELSE 'Tiny'
    END AS vehicle_size,
    
    -- Terrain capabilities
    CASE
        WHEN LOWER(vehicle_class) LIKE '%walker%' THEN 'Ground'
        WHEN LOWER(vehicle_class) LIKE '%speeder%' AND LOWER(vehicle_class) NOT LIKE '%airspeeder%' THEN 'Ground'
        WHEN LOWER(vehicle_class) LIKE '%airspeeder%' THEN 'Air'
        WHEN LOWER(vehicle_class) LIKE '%submarine%' THEN 'Water'
        WHEN LOWER(vehicle_class) LIKE '%crawler%' THEN 'Ground'
        WHEN LOWER(vehicle_class) LIKE '%barge%' AND LOWER(vehicle_class) LIKE '%sail%' THEN 'Ground/Water'
        WHEN LOWER(vehicle_class) LIKE '%snowspeeder%' OR LOWER(name) LIKE '%snow%' THEN 'Snow/Ice'
        WHEN LOWER(vehicle_class) LIKE '%repulsor%' THEN 'Air/Ground'
        ELSE 'Multi-terrain'
    END AS terrain_capability,
    
    -- Notable vehicle flag
    CASE
        WHEN LOWER(name) IN ('at-at', 'at-st', 'snowspeeder', 'speeder bike', 'tie bomber', 
                           'tie fighter', 'x-34 landspeeder', 'sand crawler', 'sail barge') THEN TRUE
        ELSE FALSE
    END AS is_notable_vehicle,
    
    -- Calculate total capacity as sum of crew and passengers
    (
        CASE
            WHEN crew ~ '^[0-9,.]+$' THEN CAST(REPLACE(crew, ',', '') AS NUMERIC)
            WHEN crew LIKE '%-%' THEN 
                CAST(SPLIT_PART(REPLACE(crew, ',', ''), '-', 1) AS NUMERIC) +
                (CAST(SPLIT_PART(REPLACE(crew, ',', ''), '-', 2) AS NUMERIC) - 
                CAST(SPLIT_PART(REPLACE(crew, ',', ''), '-', 1) AS NUMERIC)) / 2
            ELSE 0
        END +
        CASE
            WHEN passengers ~ '^[0-9,.]+$' THEN CAST(REPLACE(passengers, ',', '') AS NUMERIC)
            WHEN passengers LIKE '%-%' THEN 
                CAST(SPLIT_PART(REPLACE(passengers, ',', ''), '-', 1) AS NUMERIC) +
                (CAST(SPLIT_PART(REPLACE(passengers, ',', ''), '-', 2) AS NUMERIC) - 
                CAST(SPLIT_PART(REPLACE(passengers, ',', ''), '-', 1) AS NUMERIC)) / 2
            ELSE 0
        END
    ) AS total_capacity,
    
    -- Source URL
    url,
    
    -- ETL tracking fields (placeholders for future enhancement)
    NULL::TIMESTAMP AS fetch_timestamp,
    NULL::TIMESTAMP AS processed_timestamp,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data