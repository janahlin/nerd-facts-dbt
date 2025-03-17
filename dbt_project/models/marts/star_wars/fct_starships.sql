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
  - Enhanced with additional fields from updated staging models
*/

WITH starships AS (
    SELECT
        id AS starship_id,
        starship_name, -- Use the standardized field name from staging
        model,
        manufacturer,
        cost_in_credits, -- Already numeric in staging
        length_m, -- Already converted to numeric with _m suffix
        max_atmosphering_speed, -- Already numeric in staging
        crew_count, -- Renamed from crew to crew_count in staging
        passenger_capacity, -- Renamed from passengers in staging
        cargo_capacity, -- Already numeric in staging
        consumables,
        hyperdrive_rating, -- Already numeric in staging
        MGLT, -- Already numeric in staging
        starship_class,
        
        -- Add new fields from enhanced staging model
        film_appearances,
        film_names,
        pilot_count, -- Use directly instead of calculating
        pilot_names,
        
        -- Add new classification fields
        starship_role, -- Use pre-defined role
        starship_size, -- Use pre-defined size classification
        
        -- Add new effectiveness metrics
        is_notable_starship,
        total_capacity,
        
        -- Add source tracking
        url,
        fetch_timestamp,
        processed_timestamp
    FROM {{ ref('stg_swapi_starships') }}
    WHERE id IS NOT NULL
),

-- Calculate derived metrics for each starship
starship_metrics AS (
    SELECT
        s.*,
        
        -- Calculate crew efficiency (passengers per crew member)
        CASE
            WHEN s.crew_count > 0 THEN ROUND(s.passenger_capacity::NUMERIC / s.crew_count, 2)
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
                    -- Famous starship bonus - use the is_notable_starship field
                    CASE
                        WHEN s.is_notable_starship = TRUE THEN 15
                        ELSE 0
                    END
                ))
            ELSE 50
        END AS effectiveness_score
    FROM starships s
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
    COALESCE(sm.passenger_capacity, 0) AS passenger_count,
    COALESCE(sm.total_capacity, 0) AS total_capacity, -- Use pre-calculated field
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
    
    -- Use the is_notable_starship flag directly from staging
    sm.is_notable_starship AS is_iconic,
    
    -- Use the starship_role field directly from staging, or keep local logic for better control
    COALESCE(sm.starship_role, 
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
        END
    ) AS starship_role,
    
    -- Use the starship_size field or keep local logic for better granularity
    COALESCE(sm.starship_size,
        CASE
            WHEN sm.length_m IS NULL THEN 'Unknown'
            WHEN sm.length_m > 10000 THEN 'Massive (Station)'
            WHEN sm.length_m > 1000 THEN 'Huge (Capital Ship)'
            WHEN sm.length_m > 500 THEN 'Very Large'
            WHEN sm.length_m > 100 THEN 'Large'
            WHEN sm.length_m > 50 THEN 'Medium'
            WHEN sm.length_m > 20 THEN 'Small'
            ELSE 'Tiny'
        END
    ) AS size_class,
    
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
    
    -- Relationship counts from staging
    sm.pilot_count,
    sm.pilot_names AS notable_pilots,
    sm.film_appearances AS film_appearance_count,
    sm.film_names AS film_names_list,
    
    -- Source data metadata
    sm.url AS source_url,
    sm.fetch_timestamp,
    sm.processed_timestamp,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM starship_metrics sm
ORDER BY sm.effectiveness_score DESC, sm.starship_name