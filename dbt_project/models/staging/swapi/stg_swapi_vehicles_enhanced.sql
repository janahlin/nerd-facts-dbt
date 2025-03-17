{{
  config(
    materialized = 'view'
  )
}}

WITH raw_vehicles AS (
    SELECT * FROM {{ ref('stg_swapi_vehicles') }}
),

clean_vehicles AS (
    SELECT
        id AS vehicle_id,
        vehicle_name,
        model,
        manufacturer,
        
        -- Clean and convert numeric fields safely
        CASE 
            WHEN cost_in_credits IS NULL THEN NULL
            WHEN cost_in_credits::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN cost_in_credits::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN cost_in_credits::NUMERIC
            ELSE NULL
        END AS cost_in_credits,
        
        CASE 
            WHEN length_m IS NULL THEN NULL
            WHEN length_m::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN length_m::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN length_m::NUMERIC
            ELSE NULL
        END AS length_m,
        
        CASE 
            WHEN max_speed IS NULL THEN NULL
            WHEN max_speed::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN max_speed::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN max_speed::NUMERIC
            ELSE NULL
        END AS max_speed,
        
        CASE 
            WHEN crew_count IS NULL THEN NULL
            WHEN crew_count::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN crew_count::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN crew_count::NUMERIC
            ELSE NULL
        END AS crew_count,
        
        CASE 
            WHEN passenger_capacity IS NULL THEN NULL
            WHEN passenger_capacity::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN passenger_capacity::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN passenger_capacity::NUMERIC
            ELSE NULL
        END AS passenger_capacity,
        
        CASE 
            WHEN cargo_capacity IS NULL THEN NULL
            WHEN cargo_capacity::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN cargo_capacity::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN cargo_capacity::NUMERIC
            ELSE NULL
        END AS cargo_capacity,
        
        CASE 
            WHEN total_capacity IS NULL THEN NULL
            WHEN total_capacity::TEXT IN ('none', 'unknown', '') THEN NULL
            WHEN total_capacity::TEXT ~ '^[0-9]+(\.[0-9]+)?$' THEN total_capacity::NUMERIC
            ELSE NULL
        END AS total_capacity,
        
        -- Non-numeric fields
        consumables,
        vehicle_class,
        
        -- Additional fields
        film_appearances,
        film_names,
        pilot_count,
        pilot_names,
        vehicle_purpose,
        vehicle_size,
        terrain_capability,
        is_notable_vehicle,
        
        -- Source tracking
        url,
        fetch_timestamp,
        processed_timestamp
    FROM raw_vehicles
)

SELECT * FROM clean_vehicles