{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['starship_id']}, {'columns': ['starship_class']}, {'columns': ['hyperspace_rating']}],
    unique_key = 'starship_key'
  )
}}

/*
  Model: fct_starships
  Description: Fact table for Star Wars starships with comprehensive metrics and classifications
  
  Notes:
  - Contains detailed specifications and performance metrics for all starships
  - Adds derived attributes for battle capabilities, economic analysis, and technical specifications
  - Includes hyperdrive performance and crew efficiency calculations
  - Connects to related dimension tables for pilots, manufacturers, and classes
  - Provides context for starship roles within the Star Wars universe
*/

WITH starships AS (
    SELECT
        id AS starship_id,
        name AS starship_name,
        model,
        manufacturer,
        NULLIF(cost_in_credits, 'unknown')::NUMERIC AS cost_in_credits,
        NULLIF(length, 'unknown')::NUMERIC AS length_m,
        NULLIF(max_atmosphering_speed, 'unknown')::NUMERIC AS max_atmosphering_speed,
        NULLIF(crew, 'unknown')::NUMERIC AS crew_count,
        NULLIF(passengers, 'unknown')::NUMERIC AS passenger_count,
        NULLIF(cargo_capacity, 'unknown')::NUMERIC AS cargo_capacity,
        consumables,
        NULLIF(hyperdrive_rating, 'unknown')::NUMERIC AS hyperdrive_rating,
        NULLIF(MGLT, 'unknown')::NUMERIC AS MGLT,
        starship_class,
        pilots,
        films
    FROM {{ ref('stg_swapi_starships') }}
    WHERE id IS NOT NULL
),

-- Calculate the number of pilots and films for each starship
starship_relationships AS (
    SELECT
        starship_id,
        COALESCE(JSONB_ARRAY_LENGTH(pilots), 0) AS pilot_count,
        COALESCE(JSONB_ARRAY_LENGTH(films), 0) AS film_appearance_count
    FROM starships
),

-- Calculate derived metrics for each starship
starship_metrics AS (
    SELECT
        s.*,
        sr.pilot_count,
        sr.film_appearance_count,
        
        -- Calculate crew efficiency (passengers per crew member)
        CASE
            WHEN s.crew_count > 0 THEN ROUND(s.passenger_count::NUMERIC / s.crew_count, 2)
            ELSE 0
        END AS passengers_per_crew,
        
        -- Calculate cargo efficiency (cargo capacity per meter of length)
        CASE
            WHEN s.length_m > 0 THEN ROUND(s.cargo_capacity::NUMERIC / s.length_m, 2)
            ELSE 0
        END AS cargo_efficiency,
        
        -- Calculate hyperdrive performance score (lower is better, invert for intuitive scoring)
        CASE
            WHEN s.hyperdrive_rating > 0 THEN ROUND(100 / s.hyperdrive_rating, 1)
            ELSE 0
        END AS hyperdrive_performance_score,
        
        -- Calculate overall effectiveness score (0-100)
        CASE
            WHEN s.starship_class IS NOT NULL THEN
                GREATEST(0, LEAST(100,
                    -- Base score
                    50 +
                    -- Speed bonus
                    CASE
                        WHEN s.MGLT IS NOT NULL AND s.MGLT > 0 THEN LEAST(20, s.MGLT / 5)
                        WHEN s.max_atmosphering_speed > 1000 THEN 10
                        ELSE 0
                    END +
                    -- Hyperdrive bonus (lower rating is better)
                    CASE
                        WHEN s.hyperdrive_rating IS NOT NULL AND s.hyperdrive_rating > 0 THEN
                            CASE
                                WHEN s.hyperdrive_rating <= 1 THEN 20
                                WHEN s.hyperdrive_rating <= 2 THEN 10
                                ELSE 0
                            END
                        ELSE 0
                    END +
                    -- Size bonus for large combat ships
                    CASE
                        WHEN s.length_m > 1000 AND s.starship_class IN (
                            'Star Destroyer', 'Dreadnought', 'Battlecruiser', 'Star Dreadnought'
                        ) THEN 20
                        WHEN s.length_m > 500 THEN 10
                        ELSE 0
                    END +
                    -- Cargo capacity bonus
                    CASE
                        WHEN s.cargo_capacity > 1000000 THEN 10
                        ELSE 0
                    END +
                    -- Famous starship bonus
                    CASE
                        WHEN LOWER(s.starship_name) IN (
                            'millennium falcon', 'death star', 'executor', 'slave 1', 
                            'star destroyer', 'tantive iv', 'x-wing', 'tie fighter'
                        ) THEN 15
                        ELSE 0
                    END
                ))
            ELSE 50
        END AS effectiveness_score
    FROM starships s
    JOIN starship_relationships sr ON s.starship_id = sr.starship_id
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['sm.starship_id']) }} AS starship_key,
    
    -- Core identifiers
    sm.starship_id,
    sm.starship_name,
    sm.model,
    sm.manufacturer,
    
    -- Classification
    sm.starship_class,
    
    -- Converted hyperspace rating to classification
    CASE
        WHEN sm.hyperdrive_rating IS NULL THEN 'Unknown'
        WHEN sm.hyperdrive_rating <= 0.5 THEN 'Ultra Fast'
        WHEN sm.hyperdrive_rating <= 1.0 THEN 'Very Fast'
        WHEN sm.hyperdrive_rating <= 2.0 THEN 'Fast'
        WHEN sm.hyperdrive_rating <= 3.0 THEN 'Average'
        WHEN sm.hyperdrive_rating <= 4.0 THEN 'Slow'
        ELSE 'Very Slow'
    END AS hyperspace_rating,
    
    -- Faction affiliation based on model and name
    CASE
        WHEN LOWER(sm.starship_name) LIKE '%imperial%' OR 
             LOWER(sm.model) LIKE '%imperial%' OR 
             LOWER(sm.starship_name) LIKE '%tie%' OR
             LOWER(sm.starship_name) = 'executor' OR
             LOWER(sm.starship_name) = 'death star' THEN 'Imperial'
        
        WHEN LOWER(sm.starship_name) LIKE '%republic%' OR 
             LOWER(sm.model) LIKE '%republic%' OR 
             LOWER(sm.starship_name) LIKE '%naboo%' OR
             LOWER(sm.manufacturer) LIKE '%republic%' OR
             LOWER(sm.starship_name) = 'jedi starfighter' THEN 'Republic'
        
        WHEN LOWER(sm.starship_name) LIKE '%rebel%' OR 
             LOWER(sm.model) LIKE '%rebel%' OR
             LOWER(sm.starship_name) LIKE '%x-wing%' OR
             LOWER(sm.starship_name) LIKE '%y-wing%' OR
             LOWER(sm.starship_name) LIKE '%a-wing%' OR
             LOWER(sm.starship_name) LIKE '%b-wing%' OR
             LOWER(sm.starship_name) = 'millennium falcon' OR
             LOWER(sm.starship_name) = 'tantive iv' THEN 'Rebel Alliance/Resistance'
             
        WHEN LOWER(sm.starship_name) LIKE '%trade federation%' OR
             LOWER(sm.model) LIKE '%federation%' OR
             LOWER(sm.starship_name) LIKE '%separatist%' OR
             LOWER(sm.manufacturer) LIKE '%techno union%' THEN 'Separatist/Trade Federation'
             
        WHEN LOWER(sm.starship_name) LIKE '%first order%' OR
             LOWER(sm.model) LIKE '%first order%' THEN 'First Order'
             
        WHEN LOWER(sm.starship_name) = 'slave 1' OR
             LOWER(sm.starship_name) LIKE '%firespray%' THEN 'Bounty Hunter'
             
        ELSE 'Civilian/Neutral'
    END AS faction_affiliation,
    
    -- Physical specifications with proper handling
    COALESCE(sm.length_m, 0) AS length_m,
    COALESCE(sm.max_atmosphering_speed, 0) AS max_atmosphering_speed,
    COALESCE(sm.MGLT, 0) AS MGLT,
    COALESCE(sm.hyperdrive_rating, 0) AS hyperdrive_rating,
    
    -- Cost information
    COALESCE(sm.cost_in_credits, 0) AS cost_in_credits,
    
    -- Format costs in a readable way
    CASE
        WHEN sm.cost_in_credits >= 1000000000 THEN TRIM(TO_CHAR(sm.cost_in_credits/1000000000.0, '999,999,990.99')) || ' billion credits'
        WHEN sm.cost_in_credits >= 1000000 THEN TRIM(TO_CHAR(sm.cost_in_credits/1000000.0, '999,999,990.99')) || ' million credits'
        WHEN sm.cost_in_credits >= 1000 THEN TRIM(TO_CHAR(sm.cost_in_credits/1000.0, '999,999,990.99')) || ' thousand credits'
        WHEN sm.cost_in_credits IS NOT NULL THEN TRIM(TO_CHAR(sm.cost_in_credits, '999,999,999,999')) || ' credits'
        ELSE 'unknown'
    END AS cost_formatted,
    
    -- Value classification
    CASE
        WHEN sm.cost_in_credits IS NULL THEN 'Unknown'
        WHEN sm.cost_in_credits >= 1000000000 THEN 'Capital Investment'
        WHEN sm.cost_in_credits >= 100000000 THEN 'Military Grade'
        WHEN sm.cost_in_credits >= 10000000 THEN 'Very Expensive'
        WHEN sm.cost_in_credits >= 1000000 THEN 'Expensive'
        WHEN sm.cost_in_credits >= 100000 THEN 'Moderate'
        ELSE 'Affordable'
    END AS cost_category,
    
    -- Capacity information
    COALESCE(sm.crew_count, 0) AS crew_count,
    COALESCE(sm.passenger_count, 0) AS passenger_count,
    COALESCE(sm.crew_count, 0) + COALESCE(sm.passenger_count, 0) AS total_capacity,
    COALESCE(sm.cargo_capacity, 0) AS cargo_capacity,
    
    -- Format cargo in a readable way
    CASE
        WHEN sm.cargo_capacity >= 1000000000 THEN TRIM(TO_CHAR(sm.cargo_capacity/1000000000.0, '999,999,990.99')) || ' million tons'
        WHEN sm.cargo_capacity >= 1000000 THEN TRIM(TO_CHAR(sm.cargo_capacity/1000000.0, '999,999,990.99')) || ' tons'
        WHEN sm.cargo_capacity >= 1000 THEN TRIM(TO_CHAR(sm.cargo_capacity/1000.0, '999,999,990.99')) || ' kg'
        WHEN sm.cargo_capacity IS NOT NULL THEN TRIM(TO_CHAR(sm.cargo_capacity, '999,999,999,999')) || ' kg'
        ELSE 'unknown'
    END AS cargo_capacity_formatted,
    
    -- Consumables duration
    sm.consumables,
    
    -- Derived metrics
    sm.passengers_per_crew,
    sm.cargo_efficiency,
    sm.hyperdrive_performance_score,
    sm.effectiveness_score,
    
    -- Iconic starship flag with comprehensive list
    CASE
        WHEN LOWER(sm.starship_name) IN (
            'millennium falcon', 'death star', 'executor', 'slave 1', 
            'star destroyer', 'tantive iv', 'tie fighter', 'x-wing',
            'y-wing', 'a-wing', 'b-wing', 'tie interceptor', 'tie bomber',
            'super star destroyer', 'invisible hand', 'trade federation cruiser',
            'jedi starfighter', 'naboo royal starship', 'radiant vii',
            'arc-170', 'venator-class star destroyer', 'v-wing',
            'nebulon-b frigate', 'cr90 corvette', 'mon calamari cruiser',
            'republic attack cruiser', 'theta-class t-2c shuttle',
            'republic cruiser', 'sith infiltrator', 'solar sailer',
            'droid control ship', 'h-type nubian yacht'
        ) THEN TRUE
        ELSE FALSE
    END AS is_iconic,
    
    -- Role classification
    CASE
        WHEN sm.starship_class LIKE '%fighter%' OR 
             sm.starship_class LIKE '%interceptor%' THEN 'Combat - Fighter'
        WHEN sm.starship_class LIKE '%destroyer%' OR 
             sm.starship_class LIKE '%cruiser%' OR
             sm.starship_class LIKE '%battleship%' OR
             sm.starship_class LIKE '%dreadnought%' THEN 'Combat - Capital Ship'
        WHEN sm.starship_class LIKE '%bomber%' THEN 'Combat - Bomber'
        WHEN sm.starship_class LIKE '%transport%' THEN 'Transport'
        WHEN sm.starship_class LIKE '%shuttle%' OR 
             sm.starship_class LIKE '%yacht%' OR
             sm.starship_class LIKE '%pleasure craft%' THEN 'Personal/Diplomatic'
        WHEN sm.starship_class LIKE '%freighter%' THEN 'Cargo'
        WHEN sm.starship_class LIKE '%station%' OR 
             sm.starship_name LIKE '%death star%' THEN 'Battle Station'
        ELSE 'Multi-purpose'
    END AS starship_role,
    
    -- Size classification
    CASE
        WHEN sm.length_m IS NULL THEN 'Unknown'
        WHEN sm.length_m > 10000 THEN 'Massive (Station)'
        WHEN sm.length_m > 1000 THEN 'Huge (Capital Ship)'
        WHEN sm.length_m > 500 THEN 'Very Large'
        WHEN sm.length_m > 100 THEN 'Large'
        WHEN sm.length_m > 50 THEN 'Medium'
        WHEN sm.length_m > 20 THEN 'Small'
        ELSE 'Tiny'
    END AS size_class,
    
    -- Era classification
    CASE
        WHEN LOWER(sm.starship_name) LIKE '%republic%' OR
             LOWER(sm.model) LIKE '%republic%' OR
             LOWER(sm.starship_name) LIKE '%naboo%' OR
             LOWER(sm.starship_name) LIKE '%jedi starfighter%' OR
             LOWER(sm.starship_name) LIKE '%droid%' THEN 'Prequel Era (Clone Wars)'
             
        WHEN LOWER(sm.starship_name) LIKE '%imperial%' OR
             LOWER(sm.model) LIKE '%imperial%' OR
             LOWER(sm.starship_name) LIKE '%tie%' OR
             LOWER(sm.starship_name) = 'executor' OR
             LOWER(sm.starship_name) = 'death star' OR
             LOWER(sm.starship_name) LIKE '%x-wing%' OR 
             LOWER(sm.starship_name) LIKE '%y-wing%' OR
             LOWER(sm.starship_name) LIKE '%a-wing%' OR
             LOWER(sm.starship_name) LIKE '%b-wing%' OR
             LOWER(sm.starship_name) = 'millennium falcon' THEN 'Original Trilogy Era (Galactic Civil War)'
             
        WHEN LOWER(sm.starship_name) LIKE '%first order%' OR
             LOWER(sm.model) LIKE '%first order%' THEN 'Sequel Era (First Order Conflict)'
             
        ELSE 'Multiple Eras/Unspecified'
    END AS starship_era,
    
    -- Relationship counts
    sm.pilot_count,
    sm.film_appearance_count,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM starship_metrics sm
ORDER BY sm.effectiveness_score DESC, sm.starship_name