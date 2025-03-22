{{
  config(
    materialized = 'view'
  )
}}

with vehicles as (        
    select
    vehicle_id,    
    vehicle_name,
    model,
    manufacturer,
    vehicle_class,
    consumables,    
    cost_in_credits,
    length,
    max_atmosphering_speed,
    cargo_capacity,
    passengers,
    crew,

        -- Derived vehicle classifications
    CASE
        WHEN LOWER(vehicle_class) LIKE '%walker%' OR LOWER(vehicle_class) LIKE '%tank%' THEN 'Military'
        WHEN LOWER(vehicle_class) LIKE '%fighter%' OR LOWER(vehicle_class) LIKE '%bomber%' THEN 'Military'
        WHEN LOWER(vehicle_class) LIKE '%transport%' THEN 'Transport'
        WHEN LOWER(vehicle_class) LIKE '%speeder%' THEN 
            CASE
                WHEN LOWER(vehicle_name) LIKE '%military%' THEN 'Military'
                ELSE 'Civilian'
            END
        WHEN LOWER(vehicle_class) LIKE '%airspeeder%' THEN
            CASE
                WHEN LOWER(vehicle_name) LIKE '%military%' THEN 'Military'
                ELSE 'Civilian' 
            END
        WHEN LOWER(vehicle_class) LIKE '%shuttle%' THEN 'Transport'
        WHEN LOWER(vehicle_class) LIKE '%barge%' OR LOWER(vehicle_class) LIKE '%yacht%' THEN 'Leisure/Luxury'
        WHEN LOWER(vehicle_class) LIKE '%crawler%' OR LOWER(vehicle_class) LIKE '%digger%' THEN 'Industrial'
        ELSE 'Multipurpose'
    END AS vehicle_purpose,

    -- Size classification
    CASE
        WHEN length > 100 THEN 'Massive'
        WHEN length > 20 THEN 'Large'
        WHEN length > 10 THEN 'Medium'
        WHEN length > 5 THEN 'Small'
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
        WHEN LOWER(vehicle_class) LIKE '%snowspeeder%' OR LOWER(vehicle_name) LIKE '%snow%' THEN 'Snow/Ice'
        WHEN LOWER(vehicle_class) LIKE '%repulsor%' THEN 'Air/Ground'
        ELSE 'Multi-terrain'
    END AS terrain_capability,

    -- Notable vehicle flag
    CASE
        WHEN LOWER(vehicle_name) IN ('at-at', 'at-st', 'snowspeeder', 'speeder bike', 'tie bomber', 
                           'tie fighter', 'x-34 landspeeder', 'sand crawler', 'sail barge') THEN TRUE
        ELSE FALSE
    END AS is_notable_vehicle,    

    -- Calculate total capacity as sum of crew and passengers
    crew + passengers AS total_capacity,

    created_at,
    edited_at,
    dbt_loaded_at,
    url
    from {{ ref('stg_swapi_vehicles') }}
)

select * from vehicles
