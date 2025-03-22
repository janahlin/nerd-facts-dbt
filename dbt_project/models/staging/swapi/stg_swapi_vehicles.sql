{{
  config(
    materialized = 'view'
  )
}}

/*
  Model: stg_swapi_vehicles
  Description: Standardizes Star Wars vehicle data from SWAPI
  Source: raw.swapi_vehicles
*/

WITH raw_data AS (
    SELECT
        -- Identitfiers
        id,

        -- Text fields
        name,
        model,
        manufacturer,
        vehicle_class,
        consumables,
        url,

        -- Numeric fields
        CASE cost_in_credits~'^[0-9,.]+$' WHEN TRUE THEN cost_in_credits ELSE null END AS cost_in_credits,
        CASE length~'^[0-9,.]+$' WHEN TRUE THEN length ELSE null END AS length,        
        CASE max_atmosphering_speed~'^[0-9,.]+$' WHEN TRUE THEN max_atmosphering_speed ELSE null END AS max_atmosphering_speed,
        CASE cargo_capacity~'^[0-9,.]+$' WHEN TRUE THEN cargo_capacity ELSE null END AS cargo_capacity,
        CASE passengers~'^[0-9,.]+$' WHEN TRUE THEN passengers ELSE null END AS passengers,
        CASE crew~'^[0-9,.]+$' WHEN TRUE THEN crew ELSE null END AS crew,                        
        
        -- Relationship arrays
        pilots,
        films,
        
        -- Timestamp fields        
        created,
        edited
    FROM {{ source('swapi', 'vehicles') }}
    WHERE id IS NOT NULL
)

SELECT
    id as vehicle_id,

    -- Text fields
    name as vehicle_name,
    model,
    manufacturer,
    vehicle_class,
    consumables,
    url,

    -- Numeric fields
    CAST(cost_in_credits AS NUMERIC) AS cost_in_credits,
    CAST(length AS NUMERIC) AS length,
    CAST(max_atmosphering_speed AS NUMERIC) AS max_atmosphering_speed,
    CAST(cargo_capacity AS NUMERIC) AS cargo_capacity,
    CAST(passengers AS NUMERIC) AS passengers,
    CAST(crew AS NUMERIC) AS crew,                        
    
    -- Relationship arrays
    pilots,
    films,
    
    -- Timestamp fields        
    CAST(created AS TIMESTAMP) AS created_at,
    CAST(edited AS TIMESTAMP) AS edited_at,

    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
