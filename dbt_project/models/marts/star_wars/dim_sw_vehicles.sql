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
*/

WITH vehicles AS (
    SELECT
        id AS vehicle_id,
        name AS vehicle_name,
        model,
        manufacturer,
        NULLIF(cost_in_credits, 'unknown')::NUMERIC AS cost_in_credits,
        NULLIF(length, 'unknown')::NUMERIC AS length,
        NULLIF(max_atmosphering_speed, 'unknown')::NUMERIC AS max_atmosphering_speed,
        NULLIF(crew, 'unknown')::NUMERIC AS crew,
        NULLIF(passengers, 'unknown')::NUMERIC AS passengers,
        NULLIF(cargo_capacity, 'unknown')::NUMERIC AS cargo_capacity,
        consumables,
        vehicle_class
    FROM {{ ref('stg_swapi_vehicles') }}
),

-- Add derived attributes
vehicle_attributes AS (
    SELECT
        *,
        -- Enhanced vehicle size classification with more granularity
        CASE
            WHEN length IS NULL THEN 'Unknown'
            WHEN length < 5 THEN 'Tiny'
            WHEN length < 10 THEN 'Small'
            WHEN length < 25 THEN 'Medium'
            WHEN length < 50 THEN 'Large'
            WHEN length < 100 THEN 'Very Large'
            WHEN length < 200 THEN 'Huge'
            ELSE 'Massive'
        END AS size_class,
        
        -- Enhanced speed classification with better ranges
        CASE
            WHEN max_atmosphering_speed IS NULL THEN 'Unknown'
            WHEN max_atmosphering_speed < 50 THEN 'Very Slow'
            WHEN max_atmosphering_speed < 100 THEN 'Slow'
            WHEN max_atmosphering_speed < 300 THEN 'Moderate'
            WHEN max_atmosphering_speed < 500 THEN 'Fast'
            WHEN max_atmosphering_speed < 800 THEN 'Very Fast'
            WHEN max_atmosphering_speed < 1200 THEN 'Extremely Fast'
            ELSE 'Ultra Fast'
        END AS speed_class,
        
        -- Enhanced passenger capacity classification
        CASE
            WHEN passengers IS NULL THEN 'Unknown'
            WHEN passengers = 0 THEN 'No Passengers'
            WHEN passengers < 3 THEN 'Very Few'
            WHEN passengers < 10 THEN 'Few'
            WHEN passengers < 20 THEN 'Medium'
            WHEN passengers < 50 THEN 'Many'
            WHEN passengers < 100 THEN 'Large'
            WHEN passengers < 500 THEN 'Very Large'
            ELSE 'Massive'
        END AS passenger_capacity
    FROM vehicles
),

-- Add vehicle era, faction, and purpose information
vehicle_context AS (
    SELECT
        va.*,
        -- Vehicle purpose/role based on vehicle class and name
        CASE
            WHEN LOWER(vehicle_class) LIKE '%transport%' OR 
                 LOWER(vehicle_class) LIKE '%cargo%' THEN 'Transport'
            WHEN LOWER(vehicle_class) LIKE '%speeder%' AND 
                 (LOWER(vehicle_name) LIKE '%police%' OR
                  LOWER(vehicle_name) LIKE '%patrol%') THEN 'Law Enforcement'
            WHEN LOWER(vehicle_class) LIKE '%walker%' OR 
                 LOWER(vehicle_class) LIKE '%assault%' OR
                 LOWER(vehicle_class) LIKE '%combat%' OR
                 LOWER(vehicle_name) LIKE '%at-%' OR
                 LOWER(vehicle_name) LIKE '%fighter%' THEN 'Military'
            WHEN LOWER(vehicle_class) LIKE '%speeder%' OR 
                 LOWER(vehicle_class) LIKE '%bike%' OR
                 LOWER(vehicle_class) LIKE '%airspeeder%' THEN 'Civilian Transport'
            WHEN LOWER(vehicle_class) LIKE '%sail%' OR 
                 LOWER(vehicle_name) LIKE '%barge%' THEN 'Leisure/Luxury'
            WHEN LOWER(vehicle_class) LIKE '%submarine%' OR 
                 LOWER(vehicle_class) LIKE '%aquatic%' THEN 'Aquatic'
            WHEN LOWER(vehicle_class) LIKE '%mining%' OR 
                 LOWER(vehicle_class) LIKE '%crawler%' THEN 'Industrial'
            WHEN LOWER(vehicle_class) LIKE '%repulsor%' AND 
                 LOWER(vehicle_name) NOT LIKE '%military%' THEN 'Civilian Transport'
            ELSE 'Multipurpose'
        END AS vehicle_purpose,
        
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
        END AS vehicle_era
    FROM vehicle_attributes va
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
    
    -- Technical specifications with better type handling
    CASE WHEN vc.cost_in_credits IS NOT NULL 
         THEN vc.cost_in_credits 
         ELSE NULL
    END AS cost_in_credits,
    
    -- Format costs in a readable way
    CASE
        WHEN vc.cost_in_credits >= 1000000 THEN TRIM(TO_CHAR(vc.cost_in_credits/1000000.0, '999,999,990.9')) || ' million'
        WHEN vc.cost_in_credits >= 1000 THEN TRIM(TO_CHAR(vc.cost_in_credits/1000.0, '999,999,990.9')) || ' thousand'
        WHEN vc.cost_in_credits IS NOT NULL THEN TRIM(TO_CHAR(vc.cost_in_credits, '999,999,999,999'))
        ELSE 'unknown'
    END AS cost_formatted,
    
    -- Physical attributes
    CASE WHEN vc.length IS NOT NULL 
         THEN vc.length 
         ELSE NULL
    END AS length_m,
    
    CASE WHEN vc.max_atmosphering_speed IS NOT NULL 
         THEN vc.max_atmosphering_speed 
         ELSE NULL
    END AS max_atmosphering_speed,
    
    -- Capacity information with improved handling
    COALESCE(vc.crew, 0) AS crew_count,
    COALESCE(vc.passengers, 0) AS passenger_count,
    COALESCE(vc.crew, 0) + COALESCE(vc.passengers, 0) AS total_capacity,
    
    CASE WHEN vc.cargo_capacity IS NOT NULL 
         THEN vc.cargo_capacity 
         ELSE NULL
    END AS cargo_capacity,
    
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
    vc.size_class,
    vc.speed_class,
    vc.passenger_capacity,
    
    -- Star Wars specific context
    vc.vehicle_purpose,
    vc.faction_affiliation,
    vc.vehicle_era,
    
    -- Effectiveness rating (1-10) based on size, speed, and military utility
    CASE
        WHEN vc.vehicle_purpose = 'Military' THEN
            GREATEST(1, LEAST(10, FLOOR(
                COALESCE((
                    -- Base military vehicle score
                    5 +
                    -- Size bonus for military vehicles (bigger is better for intimidation)
                    CASE
                        WHEN vc.size_class = 'Massive' THEN 3
                        WHEN vc.size_class = 'Huge' THEN 2
                        WHEN vc.size_class = 'Very Large' THEN 1
                        WHEN vc.size_class = 'Tiny' THEN -1
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
                        WHEN vc.passenger_capacity = 'Massive' THEN 2
                        WHEN vc.passenger_capacity = 'Very Large' THEN 1
                        ELSE 0
                    END +
                    -- Luxury/special vehicle bonus
                    CASE
                        WHEN vc.vehicle_purpose = 'Leisure/Luxury' THEN 2
                        ELSE 0
                    END
                ), 5)))
    END AS effectiveness_rating,
    
    -- Expanded iconic vehicle detection with many more iconic vehicles
    CASE
        WHEN LOWER(vc.vehicle_name) IN (
            'at-at', 'at-st', 'snowspeeder', 'speeder bike',
            'imperial speeder bike', 'tie bomber', 'tie fighter', 'tie interceptor',
            'sandcrawler', 'sail barge', 'storm iv twin-pod cloud car',
            'at-te', 'laat/i', 'juggernaut', 'spha', 'hailfire droid',
            'vulture droid', 'corporate alliance tank droid',
            'tribubble bongo', 'sith speeder', 'zephyr-g swoop',
            'koro-2 exodrive airspeeder', 'xj-6 airspeeder',
            'flitknot speeder', 'v-wing airspeeder',
            'firespray-31', 'slave i', 'tantive iv', 'republic attack cruiser',
            'a-wing', 'b-wing', 'x-wing', 'y-wing', 'naboo n-1 starfighter'
        ) THEN TRUE
        ELSE FALSE
    END AS is_iconic,
    
    -- Vehicle popularity rating (1-10)
    CASE
        WHEN LOWER(vc.vehicle_name) IN ('at-at', 'at-st', 'snowspeeder', 'tie fighter', 'speeder bike') THEN 10
        WHEN LOWER(vc.vehicle_name) IN ('sandcrawler', 'sail barge', 'cloud car', 'at-te', 'laat/i') THEN 8
        WHEN LOWER(vc.vehicle_name) IN ('juggernaut', 'hailfire droid', 'vulture droid') THEN 6
        WHEN vc.is_iconic THEN 7
        WHEN vc.faction_affiliation = 'Imperial' THEN 5
        WHEN vc.faction_affiliation = 'Republic' THEN 5
        WHEN vc.faction_affiliation = 'Rebel Alliance' THEN 5
        ELSE 3
    END AS popularity_rating,
    
    -- Film appearances (episodes)
    CASE
        WHEN LOWER(vc.vehicle_name) = 'sandcrawler' THEN ARRAY[4]
        WHEN LOWER(vc.vehicle_name) IN ('at-at', 'snowspeeder') THEN ARRAY[5]
        WHEN LOWER(vc.vehicle_name) IN ('at-st', 'speeder bike') THEN ARRAY[6]
        WHEN LOWER(vc.vehicle_name) = 'sail barge' THEN ARRAY[6]
        WHEN LOWER(vc.vehicle_name) LIKE '%cloud car%' THEN ARRAY[5]
        WHEN LOWER(vc.vehicle_name) IN ('vulture droid', 'sith speeder') THEN ARRAY[1]
        WHEN LOWER(vc.vehicle_name) IN ('at-te', 'hailfire droid', 'corporate alliance tank droid', 'flitknot speeder') THEN ARRAY[2]
        WHEN LOWER(vc.vehicle_name) IN ('laat/i', 'juggernaut', 'spha') THEN ARRAY[2, 3]
        WHEN LOWER(vc.vehicle_name) LIKE '%airspeeder%' THEN ARRAY[2, 3]
        WHEN LOWER(vc.vehicle_name) = 'tribubble bongo' THEN ARRAY[1]
        ELSE NULL
    END AS film_appearances,
    
    -- Vehicle versatility score (1-10) based on multipurpose functionality
    CASE
        WHEN vc.cargo_capacity > 1000 AND vc.passengers > 10 THEN 8
        WHEN vc.vehicle_purpose = 'Multipurpose' THEN 7
        WHEN vc.cargo_capacity > 500 AND vc.passengers > 5 THEN 6
        WHEN vc.vehicle_purpose = 'Transport' THEN 5
        WHEN vc.vehicle_purpose = 'Military' THEN 3
        ELSE 4
    END AS versatility_score,
    
    -- Add another attribute like "stealth capability" for military vehicles
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
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM vehicle_context vc
WHERE vc.vehicle_id IS NOT NULL
ORDER BY vc.faction_affiliation, effectiveness_rating DESC, vc.vehicle_name