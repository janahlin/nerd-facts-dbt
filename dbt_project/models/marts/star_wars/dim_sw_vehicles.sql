{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['vehicle_id']}, {'columns': ['vehicle_name']}, {'columns': ['vehicle_class']}],
    unique_key = 'vehicle_key'
  )
}}

/*
  Model: dim_sw_vehicles
  Description: Dimension table for Star Wars vehicles
  
  Notes:
  - Contains comprehensive vehicle data from the Star Wars universe
  - Provides specifications, classifications, and performance metrics
  - Categorizes vehicles by era, affiliation, and purpose
  - Adds derived attributes about role and significance
  - Includes vehicle-specific context and lore details
  - Enhanced with additional fields from updated staging models
*/

WITH vehicles AS (
    SELECT
        id AS vehicle_id,
        vehicle_name,
        model,
        manufacturer,
        cost_in_credits,
        length_m,
        max_speed,
        crew_count,
        passenger_capacity,
        cargo_capacity,
        consumables,
        vehicle_class,
        
        -- Add new fields from enhanced staging model
        film_appearances,
        film_names,
        pilot_count,
        pilot_names,
        vehicle_purpose,
        vehicle_size,
        terrain_capability,
        is_notable_vehicle,
        total_capacity,
        
        -- Add source tracking
        url,
        fetch_timestamp,
        processed_timestamp
    FROM {{ ref('stg_swapi_vehicles') }}
),

-- Add vehicle era and faction information (still needed since not in staging)
vehicle_context AS (
    SELECT
        v.*,
        -- Vehicle faction affiliation based on known vehicles
        CASE
            WHEN LOWER(vehicle_name) LIKE '%imperial%' OR
                 LOWER(vehicle_name) IN ('at-at', 'at-st', 'at-dp', 'tie', 'tie bomber',
                                       'tie fighter', 'tie interceptor', 'tie/ln starfighter',
                                       'storm iv twin-pod cloud car', 'sentinel-class landing craft') OR
                 LOWER(model) LIKE '%imperial%' THEN 'Imperial'
                 
            WHEN LOWER(vehicle_name) LIKE '%republic%' OR
                 LOWER(vehicle_name) IN ('laat/i', 'at-te', 'spha', 'juggernaut', 
                                       'flitknot speeder', 'hailfire droid', 
                                       'corporate alliance tank droid') OR
                 LOWER(model) LIKE '%republic%' THEN 'Republic'
                 
            WHEN LOWER(vehicle_name) LIKE '%rebel%' OR
                 LOWER(vehicle_name) IN ('snowspeeder', 't-47 airspeeder', 
                                       'rebel alliance snowspeeder', 'tantive iv') OR
                 LOWER(model) LIKE '%rebel%' THEN 'Rebel Alliance'
                 
            WHEN LOWER(vehicle_name) LIKE '%separatist%' OR
                 LOWER(manufacturer) LIKE '%techno union%' OR
                 LOWER(vehicle_name) LIKE '%droid%' THEN 'Separatist'
                 
            WHEN LOWER(vehicle_name) LIKE '%first order%' OR
                 LOWER(model) LIKE '%first order%' THEN 'First Order'
            
            WHEN LOWER(vehicle_name) LIKE '%resistance%' OR
                 LOWER(model) LIKE '%resistance%' THEN 'Resistance'
            
            WHEN LOWER(manufacturer) LIKE '%hutt%' OR
                 LOWER(vehicle_name) LIKE '%barge%' AND 
                 LOWER(vehicle_name) LIKE '%sail%' THEN 'Hutt Cartel/Criminal'
                 
            ELSE 'Civilian/Neutral'
        END AS faction_affiliation,
        
        -- Vehicle era based on known vehicles and factions
        CASE
            WHEN LOWER(vehicle_name) LIKE '%republic%' OR
                 LOWER(manufacturer) LIKE '%kuat drive yards%' AND
                 LOWER(vehicle_name) IN ('laat/i', 'at-te', 'spha', 'juggernaut') OR
                 LOWER(vehicle_name) LIKE '%droid%' THEN 'Prequel Era (Clone Wars)'
                 
            WHEN LOWER(vehicle_name) LIKE '%imperial%' OR
                 LOWER(vehicle_name) IN ('at-at', 'at-st', 'tie bomber', 'tie fighter',
                                       'snowspeeder', 'cloud car', 'sail barge') OR
                 LOWER(vehicle_name) LIKE '%rebel%' THEN 'Original Trilogy Era (Galactic Civil War)'
                 
            WHEN LOWER(vehicle_name) LIKE '%first order%' OR
                 LOWER(vehicle_name) LIKE '%resistance%' THEN 'Sequel Era (First Order Conflict)'
                 
            ELSE 'Multiple Eras/Unspecified'
        END AS vehicle_era,
        
        -- Enhanced speed classification with better ranges - using max_speed field
        CASE
            WHEN max_speed IS NULL THEN 'Unknown'
            WHEN max_speed < 50 THEN 'Very Slow'
            WHEN max_speed < 100 THEN 'Slow'
            WHEN max_speed < 300 THEN 'Moderate'
            WHEN max_speed < 500 THEN 'Fast'
            WHEN max_speed < 800 THEN 'Very Fast'
            WHEN max_speed < 1200 THEN 'Extremely Fast'
            ELSE 'Ultra Fast'
        END AS speed_class
    FROM vehicles v
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['vc.vehicle_id']) }} AS vehicle_key,
    
    -- Core identifiers
    vc.vehicle_id,
    vc.vehicle_name,
    
    -- Manufacturer details
    vc.model,
    vc.manufacturer,
    
    -- Technical specifications
    vc.cost_in_credits,
    
    -- Format costs in a readable way
    CASE
        WHEN vc.cost_in_credits >= 1000000 THEN TRIM(TO_CHAR(vc.cost_in_credits/1000000.0, '999,999,990.9')) || ' million'
        WHEN vc.cost_in_credits >= 1000 THEN TRIM(TO_CHAR(vc.cost_in_credits/1000.0, '999,999,990.9')) || ' thousand'
        WHEN vc.cost_in_credits IS NOT NULL THEN TRIM(TO_CHAR(vc.cost_in_credits, '999,999,999,999'))
        ELSE 'unknown'
    END AS cost_formatted,
    
    -- Physical attributes
    vc.length_m,
    vc.max_speed,
    
    -- Capacity information
    vc.crew_count,
    vc.passenger_capacity AS passenger_count,
    vc.total_capacity,
    vc.cargo_capacity,
    
    -- Format cargo in a readable way
    CASE
        WHEN vc.cargo_capacity >= 1000000 THEN TRIM(TO_CHAR(vc.cargo_capacity/1000000.0, '999,999,990.9')) || ' tons'
        WHEN vc.cargo_capacity >= 1000 THEN TRIM(TO_CHAR(vc.cargo_capacity/1000.0, '999,999,990.9')) || ' kg'
        WHEN vc.cargo_capacity IS NOT NULL THEN TRIM(TO_CHAR(vc.cargo_capacity, '999,999,999,999')) || ' kg'
        ELSE 'unknown'
    END AS cargo_capacity_formatted,
    
    -- Operational information
    vc.consumables,
    
    -- Classification fields with better organization
    vc.vehicle_class,
    vc.vehicle_size AS size_class,
    vc.speed_class,
    
    -- Passenger capacity classification from staging model
    CASE
        WHEN vc.passenger_capacity IS NULL THEN 'Unknown'
        WHEN vc.passenger_capacity = 0 THEN 'No Passengers'
        WHEN vc.passenger_capacity < 3 THEN 'Very Few'
        WHEN vc.passenger_capacity < 10 THEN 'Few'
        WHEN vc.passenger_capacity < 20 THEN 'Medium'
        WHEN vc.passenger_capacity < 50 THEN 'Many'
        WHEN vc.passenger_capacity < 100 THEN 'Large'
        WHEN vc.passenger_capacity < 500 THEN 'Very Large'
        ELSE 'Massive'
    END AS passenger_capacity_class,
    
    -- Star Wars specific context
    vc.vehicle_purpose,
    vc.faction_affiliation,
    vc.vehicle_era,
    
    -- Terrain capability from staging
    vc.terrain_capability,
    
    -- Pilot information from staging
    vc.pilot_count AS known_pilot_count,
    vc.pilot_names AS notable_pilots,
    CASE
        WHEN vc.pilot_count > 0 THEN TRUE
        ELSE FALSE
    END AS has_known_pilots,
    
    -- Effectiveness rating (1-10) based on size, speed, and military utility
    CASE
        WHEN vc.vehicle_purpose = 'Military' THEN
            GREATEST(1, LEAST(10, FLOOR(
                COALESCE((
                    -- Base military vehicle score
                    5 +
                    -- Size bonus for military vehicles (bigger is better for intimidation)
                    CASE
                        WHEN vc.vehicle_size = 'Massive' THEN 3
                        WHEN vc.vehicle_size = 'Huge' THEN 2
                        WHEN vc.vehicle_size = 'Very Large' THEN 1
                        WHEN vc.vehicle_size = 'Tiny' THEN -1
                        ELSE 0
                    END +
                    -- Speed bonus (faster is better for military)
                    CASE
                        WHEN vc.speed_class = 'Ultra Fast' THEN 3
                        WHEN vc.speed_class = 'Extremely Fast' THEN 2
                        WHEN vc.speed_class = 'Very Fast' THEN 1
                        WHEN vc.speed_class = 'Very Slow' THEN -1
                        ELSE 0
                    END +
                    -- Known effective military vehicles bonus
                    CASE
                        WHEN LOWER(vc.vehicle_name) IN ('at-at', 'at-te', 'juggernaut') THEN 2
                        ELSE 0
                    END
                ), 5)))
        ELSE
            -- Non-military vehicles rated on different criteria
            GREATEST(1, LEAST(10, FLOOR(
                COALESCE((
                    -- Base civilian vehicle score
                    5 +
                    -- Speed bonus (faster is better for civilian)
                    CASE
                        WHEN vc.speed_class = 'Ultra Fast' THEN 3
                        WHEN vc.speed_class = 'Extremely Fast' THEN 2
                        WHEN vc.speed_class = 'Very Fast' THEN 1
                        ELSE 0
                    END +
                    -- Passenger capacity bonus
                    CASE
                        WHEN vc.passenger_capacity >= 500 THEN 2
                        WHEN vc.passenger_capacity >= 100 THEN 1
                        ELSE 0
                    END +
                    -- Luxury/special vehicle bonus
                    CASE
                        WHEN vc.vehicle_purpose = 'Leisure/Luxury' THEN 2
                        ELSE 0
                    END
                ), 5)))
    END AS effectiveness_rating,
    
    -- Use the is_notable_vehicle flag directly from staging
    vc.is_notable_vehicle AS is_iconic,
    
    -- Vehicle popularity rating (1-10)
    CASE
        WHEN LOWER(vc.vehicle_name) IN ('at-at', 'at-st', 'snowspeeder', 'tie fighter', 'speeder bike') THEN 10
        WHEN LOWER(vc.vehicle_name) IN ('sandcrawler', 'sail barge', 'cloud car', 'at-te', 'laat/i') THEN 8
        WHEN LOWER(vc.vehicle_name) IN ('juggernaut', 'hailfire droid', 'vulture droid') THEN 6
        WHEN vc.is_notable_vehicle THEN 7
        WHEN vc.faction_affiliation = 'Imperial' THEN 5
        WHEN vc.faction_affiliation = 'Republic' THEN 5
        WHEN vc.faction_affiliation = 'Rebel Alliance' THEN 5
        ELSE 3
    END AS popularity_rating,
    
    -- Film appearances from staging model
    vc.film_appearances AS film_appearance_count,
    vc.film_names AS film_appearances_list,
    
    -- Vehicle versatility score (1-10) based on multipurpose functionality
    CASE
        WHEN vc.cargo_capacity > 1000 AND vc.passenger_capacity > 10 THEN 8
        WHEN vc.vehicle_purpose = 'Multipurpose' THEN 7
        WHEN vc.cargo_capacity > 500 AND vc.passenger_capacity > 5 THEN 6
        WHEN vc.vehicle_purpose = 'Transport' THEN 5
        WHEN vc.vehicle_purpose = 'Military' THEN 3
        ELSE 4
    END AS versatility_score,
    
    -- Add stealth capability for military vehicles
    CASE
        WHEN vc.vehicle_purpose = 'Military' AND
             (LOWER(vc.vehicle_name) LIKE '%scout%' OR
              LOWER(vc.vehicle_name) LIKE '%speeder bike%' OR
              LOWER(vc.model) LIKE '%recon%') THEN 'High Stealth'
        WHEN vc.vehicle_purpose = 'Military' AND
             (LOWER(vc.vehicle_name) LIKE '%at-%' OR
              LOWER(vc.vehicle_name) LIKE '%walker%') THEN 'Low Stealth'
        WHEN vc.vehicle_purpose = 'Military' THEN 'Moderate Stealth'
        ELSE 'Not Applicable'
    END AS stealth_capability,
    
    -- Source data metadata
    vc.url AS source_url,
    vc.fetch_timestamp,
    vc.processed_timestamp,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM vehicle_context vc
WHERE vc.vehicle_id IS NOT NULL
ORDER BY vc.faction_affiliation, effectiveness_rating DESC, vc.vehicle_name