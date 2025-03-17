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
*/

WITH starships AS (
    SELECT
        id AS starship_id,
        starship_name,
        model,
        manufacturer,
        cost_in_credits,
        length_m,
        max_speed,
        crew_count,
        passenger_capacity,
        cargo_capacity,
        consumables,
        hyperdrive_rating,
        MGLT,
        starship_class,
        
        -- Add new fields from enhanced staging model that we know exist
        film_appearances,
        film_names,
        pilot_count,
        pilot_names,
        
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
        
        -- Add iconic starship flag since it doesn't exist in staging
        CASE 
            WHEN LOWER(s.starship_name) IN (
                'millennium falcon', 'x-wing', 'tie fighter', 'star destroyer', 
                'death star', 'slave 1', 'executor', 'tantive iv'
            ) THEN TRUE
            ELSE FALSE
        END AS is_iconic,
        
        -- Calculate total capacity as a new field - explicit casting for safety
        (CASE WHEN s.crew_count ~ '^[0-9]+(\.[0-9]+)?$' THEN s.crew_count::NUMERIC ELSE 0 END) + 
        (CASE WHEN s.passenger_capacity ~ '^[0-9]+(\.[0-9]+)?$' THEN s.passenger_capacity::NUMERIC ELSE 0 END) 
        AS total_capacity,
        
        -- Calculate crew efficiency (passengers per crew member) - with explicit casting
        CASE
            WHEN s.crew_count ~ '^[0-9]+(\.[0-9]+)?$' AND s.crew_count::NUMERIC > 0
            THEN 
                CASE 
                    WHEN s.passenger_capacity ~ '^[0-9]+(\.[0-9]+)?$' 
                    THEN ROUND(s.passenger_capacity::NUMERIC / s.crew_count::NUMERIC, 2)
                    ELSE 0
                END
            ELSE 0
        END AS passengers_per_crew,
        
        -- Calculate cargo efficiency (cargo capacity per meter of length) - with explicit casting
        CASE
            WHEN s.length_m ~ '^[0-9]+(\.[0-9]+)?$' AND s.length_m::NUMERIC > 0
            THEN
                CASE 
                    WHEN s.cargo_capacity ~ '^[0-9]+(\.[0-9]+)?$'
                    THEN ROUND(s.cargo_capacity::NUMERIC / s.length_m::NUMERIC, 2)
                    ELSE 0
                END
            ELSE 0
        END AS cargo_efficiency,
        
        -- Calculate hyperdrive performance score (lower is better) - with explicit casting
        CASE
            WHEN s.hyperdrive_rating ~ '^[0-9]+(\.[0-9]+)?$' AND s.hyperdrive_rating::NUMERIC > 0 
            THEN ROUND(100 / s.hyperdrive_rating::NUMERIC, 1)
            ELSE 0
        END AS hyperdrive_performance_score,
        
        -- Calculate overall effectiveness score (0-100) - with safe casting
        CASE
            WHEN s.starship_class IS NOT NULL THEN
                GREATEST(0, LEAST(100,
                    -- Base score
                    50 +
                    -- Speed bonus
                    CASE
                        WHEN s.MGLT ~ '^[0-9]+(\.[0-9]+)?$' AND s.MGLT::NUMERIC > 0 
                        THEN LEAST(20, s.MGLT::NUMERIC / 5)
                        WHEN s.max_speed ~ '^[0-9]+(\.[0-9]+)?$' AND s.max_speed::NUMERIC > 1000 
                        THEN 10
                        ELSE 0
                    END +
                    -- Hyperdrive bonus (lower rating is better)
                    CASE
                        WHEN s.hyperdrive_rating ~ '^[0-9]+(\.[0-9]+)?$' AND s.hyperdrive_rating::NUMERIC > 0 THEN
                            CASE
                                WHEN s.hyperdrive_rating::NUMERIC <= 1 THEN 20
                                WHEN s.hyperdrive_rating::NUMERIC <= 2 THEN 10
                                ELSE 0
                            END
                        ELSE 0
                    END +
                    -- Size bonus for large combat ships
                    CASE
                        WHEN s.length_m ~ '^[0-9]+(\.[0-9]+)?$' AND s.length_m::NUMERIC > 1000 
                        AND s.starship_class IN (
                            'Star Destroyer', 'Dreadnought', 'Battlecruiser', 'Star Dreadnought'
                        ) THEN 20
                        WHEN s.length_m ~ '^[0-9]+(\.[0-9]+)?$' AND s.length_m::NUMERIC > 500 THEN 10
                        ELSE 0
                    END +
                    -- Cargo capacity bonus
                    CASE
                        WHEN s.cargo_capacity ~ '^[0-9]+(\.[0-9]+)?$' AND s.cargo_capacity::NUMERIC > 1000000 THEN 10
                        ELSE 0
                    END +
                    -- Famous starship bonus
                    CASE
                        WHEN LOWER(s.starship_name) IN (
                            'millennium falcon', 'x-wing', 'tie fighter', 'star destroyer', 
                            'death star', 'slave 1', 'executor', 'tantive iv'
                        ) THEN 15
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
    
    -- Converted hyperspace rating to classification - safe casting
    CASE
        WHEN NOT (sm.hyperdrive_rating ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN sm.hyperdrive_rating::NUMERIC <= 0.5 THEN 'Ultra Fast'
        WHEN sm.hyperdrive_rating::NUMERIC <= 1.0 THEN 'Very Fast'
        WHEN sm.hyperdrive_rating::NUMERIC <= 2.0 THEN 'Fast'
        WHEN sm.hyperdrive_rating::NUMERIC <= 3.0 THEN 'Average'
        WHEN sm.hyperdrive_rating::NUMERIC <= 4.0 THEN 'Slow'
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
    CASE WHEN sm.length_m ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.length_m::NUMERIC ELSE 0 END AS length_m,
    CASE WHEN sm.max_speed ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.max_speed::NUMERIC ELSE 0 END AS max_atmosphering_speed,
    CASE WHEN sm.MGLT ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.MGLT::NUMERIC ELSE 0 END AS MGLT,
    CASE WHEN sm.hyperdrive_rating ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.hyperdrive_rating::NUMERIC ELSE 0 END AS hyperdrive_rating,
    
    -- Cost information with safe casting
    CASE WHEN sm.cost_in_credits ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.cost_in_credits::NUMERIC ELSE 0 END AS cost_in_credits,
    
    -- Format costs in a readable way
    CASE
        WHEN NOT (sm.cost_in_credits ~ '^[0-9]+(\.[0-9]+)?$') THEN 'unknown'
        WHEN sm.cost_in_credits::NUMERIC >= 1000000000 THEN TRIM(TO_CHAR(sm.cost_in_credits::NUMERIC/1000000000.0, '999,999,990.99')) || ' billion credits'
        WHEN sm.cost_in_credits::NUMERIC >= 1000000 THEN TRIM(TO_CHAR(sm.cost_in_credits::NUMERIC/1000000.0, '999,999,990.99')) || ' million credits'
        WHEN sm.cost_in_credits::NUMERIC >= 1000 THEN TRIM(TO_CHAR(sm.cost_in_credits::NUMERIC/1000.0, '999,999,990.99')) || ' thousand credits'
        ELSE TRIM(TO_CHAR(sm.cost_in_credits::NUMERIC, '999,999,999,999')) || ' credits'
    END AS cost_formatted,
    
    -- Value classification - safe casting
    CASE
        WHEN NOT (sm.cost_in_credits ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN sm.cost_in_credits::NUMERIC >= 1000000000 THEN 'Capital Investment'
        WHEN sm.cost_in_credits::NUMERIC >= 100000000 THEN 'Military Grade'
        WHEN sm.cost_in_credits::NUMERIC >= 10000000 THEN 'Very Expensive'
        WHEN sm.cost_in_credits::NUMERIC >= 1000000 THEN 'Expensive'
        WHEN sm.cost_in_credits::NUMERIC >= 100000 THEN 'Moderate'
        ELSE 'Affordable'
    END AS cost_category,
    
    -- Capacity information - safe casting
    CASE WHEN sm.crew_count ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.crew_count::NUMERIC ELSE 0 END AS crew_count,
    CASE WHEN sm.passenger_capacity ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.passenger_capacity::NUMERIC ELSE 0 END AS passenger_count,
    sm.total_capacity,
    CASE WHEN sm.cargo_capacity ~ '^[0-9]+(\.[0-9]+)?$' THEN sm.cargo_capacity::NUMERIC ELSE 0 END AS cargo_capacity,
    
    -- Format cargo in a readable way - with safe casting
    CASE
        WHEN NOT (sm.cargo_capacity ~ '^[0-9]+(\.[0-9]+)?$') THEN 'unknown'
        WHEN sm.cargo_capacity::NUMERIC >= 1000000000 THEN TRIM(TO_CHAR(sm.cargo_capacity::NUMERIC/1000000000.0, '999,999,990.99')) || ' million tons'
        WHEN sm.cargo_capacity::NUMERIC >= 1000000 THEN TRIM(TO_CHAR(sm.cargo_capacity::NUMERIC/1000000.0, '999,999,990.99')) || ' tons'
        WHEN sm.cargo_capacity::NUMERIC >= 1000 THEN TRIM(TO_CHAR(sm.cargo_capacity::NUMERIC/1000.0, '999,999,990.99')) || ' kg'
        ELSE TRIM(TO_CHAR(sm.cargo_capacity::NUMERIC, '999,999,999,999')) || ' kg'
    END AS cargo_capacity_formatted,
    
    -- Consumables duration
    sm.consumables,
    
    -- Derived metrics
    sm.passengers_per_crew,
    sm.cargo_efficiency,
    sm.hyperdrive_performance_score,
    sm.effectiveness_score,
    
    -- Use our own iconic flag
    sm.is_iconic,
    
    -- Calculate starship_role directly since it doesn't exist in staging
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
    
    -- Calculate size_class directly - with safe casting
    CASE
        WHEN NOT (sm.length_m ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN sm.length_m::NUMERIC > 10000 THEN 'Massive (Station)'
        WHEN sm.length_m::NUMERIC > 1000 THEN 'Huge (Capital Ship)'
        WHEN sm.length_m::NUMERIC > 500 THEN 'Very Large'
        WHEN sm.length_m::NUMERIC > 100 THEN 'Large'
        WHEN sm.length_m::NUMERIC > 50 THEN 'Medium'
        WHEN sm.length_m::NUMERIC > 20 THEN 'Small'
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