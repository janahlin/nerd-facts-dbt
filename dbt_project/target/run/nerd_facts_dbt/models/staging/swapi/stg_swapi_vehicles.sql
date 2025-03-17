
  create view "nerd_facts"."public"."stg_swapi_vehicles__dbt_tmp"
    
    
  as (
    

/*
  Model: stg_swapi_vehicles
  Description: Standardizes Star Wars vehicle data from SWAPI
  Source: raw.swapi_vehicles
  
  Notes:
  - Numeric fields are cleaned and converted to proper types
  - Speeds and capacities have proper error handling
  - Pilot and film references are extracted as counts
  - Additional derived fields help with vehicle classification
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        model,
        manufacturer,
        NULLIF(cost_in_credits, 'unknown') AS cost_in_credits,
        NULLIF(length, 'unknown') AS length,
        NULLIF(max_atmosphering_speed, 'unknown') AS max_atmosphering_speed,
        NULLIF(crew, 'unknown') AS crew,
        NULLIF(passengers, 'unknown') AS passengers,
        NULLIF(cargo_capacity, 'unknown') AS cargo_capacity,
        consumables,
        vehicle_class,
        CASE WHEN pilots IS NULL OR pilots = '' THEN NULL::jsonb ELSE pilots::jsonb END AS pilots,
        CASE WHEN films IS NULL OR films = '' THEN NULL::jsonb ELSE films::jsonb END AS films,
        created,
        edited,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM "nerd_facts"."raw"."swapi_vehicles"
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS vehicle_name,
    model,
    manufacturer,
    vehicle_class,
    
    -- Technical specifications with proper error handling
    CASE 
        WHEN lower(cost_in_credits) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(cost_in_credits, '[^0-9]', '', 'g'), '')::NUMERIC
    END AS cost_in_credits,
    
    CASE 
        WHEN lower(length) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(length, '[^0-9\.]', '', 'g'), '')::NUMERIC
    END AS length_m,
    
    CASE 
        WHEN lower(max_atmosphering_speed) = 'unknown' THEN NULL
        WHEN lower(max_atmosphering_speed) = 'n/a' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(max_atmosphering_speed, '[^0-9]+.*', '', 'g'), '')::INTEGER 
    END AS max_speed,
    
    -- Capacity information with proper type conversion
    CASE 
        WHEN lower(crew) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(crew, '[^0-9]', '', 'g'), '')::INTEGER
    END AS crew_count,
    
    CASE 
        WHEN lower(passengers) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(passengers, '[^0-9]', '', 'g'), '')::INTEGER
    END AS passenger_capacity,
    
    CASE 
        WHEN lower(cargo_capacity) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(cargo_capacity, '[^0-9]', '', 'g'), '')::NUMERIC
    END AS cargo_capacity,
    
    consumables,
    
    -- Entity counts with error handling
    COALESCE(jsonb_array_length(pilots), 0) AS pilot_count,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage
    pilots,
    films,
    
    -- Derived vehicle classifications
    CASE
        WHEN lower(vehicle_class) IN ('assault', 'combat', 'battle') 
             OR lower(name) LIKE '%fighter%' 
             OR lower(name) LIKE '%tank%'
             OR lower(name) LIKE '%cannon%' THEN 'Military'
        WHEN lower(vehicle_class) IN ('transport', 'cargo', 'container') 
             OR lower(name) LIKE '%transport%'
             OR lower(name) LIKE '%freighter%' THEN 'Transport'
        WHEN lower(vehicle_class) IN ('speeder', 'airspeeder', 'swoop', 'landspeeder') 
             OR lower(name) LIKE '%speeder%' THEN 'Personal'
        ELSE 'Other'
    END AS vehicle_purpose,
    
    -- Size classification
    CASE
        WHEN length IS NULL THEN 'Unknown'
        WHEN length::NUMERIC < 10 THEN 'Small'
        WHEN length::NUMERIC < 25 THEN 'Medium'
        WHEN length::NUMERIC < 100 THEN 'Large'
        ELSE 'Very Large'
    END AS vehicle_size,
    
    -- Terrain capability
    CASE
        WHEN lower(vehicle_class) LIKE '%walker%' 
             OR lower(name) LIKE '%walker%' THEN 'Ground Only'
        WHEN lower(vehicle_class) LIKE '%speeder%'
             OR lower(name) LIKE '%speeder%' THEN 'Ground & Low Altitude'
        WHEN lower(vehicle_class) LIKE '%airspeeder%'
             OR lower(name) LIKE '%airspeeder%' THEN 'Aerial'
        ELSE 'Unknown'
    END AS terrain_capability,
    
    -- Notable vehicle flag
    CASE
        WHEN name IN ('AT-AT', 'AT-ST', 'Snowspeeder', 'Speeder bike',
                     'Sand Crawler', 'TIE bomber', 'Imperial Speeder Bike') 
        THEN TRUE
        ELSE FALSE
    END AS is_notable_vehicle,
    
    -- Total capacity (crew + passengers)
    COALESCE(
        NULLIF(REGEXP_REPLACE(crew, '[^0-9]', '', 'g'), '')::INTEGER, 0
    ) + 
    COALESCE(
        NULLIF(REGEXP_REPLACE(passengers, '[^0-9]', '', 'g'), '')::INTEGER, 0
    ) AS total_capacity,
    
    -- API metadata
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
  );