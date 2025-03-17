

/*
  Model: stg_swapi_starships
  Description: Standardizes Star Wars starship data from SWAPI
  Source: raw.swapi_starships
  
  Notes:
  - Numeric fields are cleaned and converted to proper types
  - Speeds and capacities have proper error handling
  - Pilot and film references are extracted as counts
  - Additional derived fields help with starship classification
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
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
        hyperdrive_rating,
        MGLT,
        starship_class,
        pilots,
        films,
        created,
        edited,
        url
    FROM raw.swapi_starships
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS starship_name,
    model,
    manufacturer,
    starship_class,
    
    -- Technical specifications with improved error handling
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
    
    CASE
        WHEN lower(hyperdrive_rating) = 'unknown' THEN NULL
        WHEN lower(hyperdrive_rating) = 'n/a' THEN NULL
        ELSE NULLIF(hyperdrive_rating, '')::NUMERIC
    END AS hyperdrive_rating,
    
    CASE
        WHEN lower(MGLT) = 'unknown' THEN NULL
        WHEN lower(MGLT) = 'n/a' THEN NULL
        ELSE NULLIF(MGLT, '')::INTEGER
    END AS mglt,
    
    -- Capacity information
    NULLIF(REGEXP_REPLACE(crew, '[^0-9]', '', 'g'), '')::INTEGER AS crew_count,
    NULLIF(REGEXP_REPLACE(passengers, '[^0-9]', '', 'g'), '')::INTEGER AS passenger_capacity,
    
    CASE 
        WHEN lower(cargo_capacity) = 'unknown' THEN NULL
        ELSE NULLIF(REGEXP_REPLACE(cargo_capacity, '[^0-9]', '', 'g'), '')::NUMERIC
    END AS cargo_capacity,
    
    consumables,
    
    -- Entity counts with error handling
    COALESCE(jsonb_array_length(pilots), 0) AS pilot_count,
    COALESCE(jsonb_array_length(films), 0) AS film_appearances,
    
    -- Keep raw arrays for downstream usage
    CASE WHEN pilots IS NULL OR pilots = '' THEN NULL::jsonb ELSE pilots::jsonb END AS pilots,
    CASE WHEN films IS NULL OR films = '' THEN NULL::jsonb ELSE films::jsonb END AS films,
    
    -- Derived ship classification
    CASE
        WHEN lower(starship_class) IN ('corvette', 'frigate', 'star destroyer', 'dreadnought')
            OR lower(name) LIKE '%star destroyer%' THEN 'Military'
        WHEN lower(starship_class) IN ('transport', 'freighter', 'yacht') 
            OR lower(name) LIKE '%transport%' THEN 'Commercial'
        WHEN lower(starship_class) IN ('starfighter', 'bomber', 'assault ship')
            OR lower(name) LIKE '%fighter%' THEN 'Starfighter'
        ELSE 'Other'
    END AS ship_purpose,
    
    -- Size classification
    CASE
        WHEN length::NUMERIC < 30 THEN 'Small'
        WHEN length::NUMERIC < 100 THEN 'Medium'
        WHEN length::NUMERIC < 500 THEN 'Large'
        WHEN length::NUMERIC < 1000 THEN 'Very Large'
        WHEN length::NUMERIC >= 1000 THEN 'Capital'
        ELSE 'Unknown'
    END AS ship_size,
    
    -- Notable ship flag
    CASE
        WHEN name IN ('Millennium Falcon', 'Death Star', 'Star Destroyer', 
                     'X-wing', 'TIE Advanced x1', 'Executor', 'Slave 1') 
        THEN TRUE
        ELSE FALSE
    END AS is_notable_ship,
    
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
    
    -- Add data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data