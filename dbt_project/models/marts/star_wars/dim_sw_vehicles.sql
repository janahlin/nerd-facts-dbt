{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['vehicle_id']}, {'columns': ['vehicle_name']}],
    unique_key = 'vehicle_key'
  )
}}

-- Get only string data from staging (no numeric conversions)
WITH base_vehicles AS (
  SELECT
    id AS vehicle_id,
    vehicle_name,
    model,
    manufacturer,
    vehicle_class,
    consumables,
    film_appearances,
    film_names,
    pilot_count,
    pilot_names,
    vehicle_purpose,
    vehicle_size,
    terrain_capability,
    is_notable_vehicle,
    url,
    fetch_timestamp,
    processed_timestamp
  FROM {{ ref('stg_swapi_vehicles') }}
),

-- Add context data without trying numeric conversions
vehicle_context AS (
  SELECT
    v.*,
    
    -- Faction affiliation  
    CASE
      WHEN LOWER(v.vehicle_name) LIKE '%imperial%' OR
           LOWER(v.vehicle_name) IN ('at-at', 'at-st', 'at-dp', 'tie', 'tie bomber',
                                 'tie fighter', 'tie interceptor', 'tie/ln starfighter') THEN 'Imperial'
      WHEN LOWER(v.vehicle_name) LIKE '%republic%' THEN 'Republic'
      WHEN LOWER(v.vehicle_name) LIKE '%rebel%' THEN 'Rebel Alliance'
      ELSE 'Civilian/Neutral'
    END AS faction_affiliation,
    
    -- Calculate simple effectiveness
    CASE 
      WHEN v.vehicle_purpose = 'Military' THEN
        GREATEST(1, LEAST(10, 5 + 
          CASE
            WHEN v.vehicle_size = 'Massive' THEN 3
            WHEN v.vehicle_size = 'Huge' THEN 2
            WHEN v.vehicle_size = 'Very Large' THEN 1
            WHEN v.vehicle_size = 'Tiny' THEN -1
            ELSE 0
          END
        ))
      ELSE 5 -- Default score
    END AS effectiveness_rating
    
  FROM base_vehicles v
)

-- Final output with surrogate key and only safe fields
SELECT 
  {{ dbt_utils.generate_surrogate_key(['v.vehicle_id']) }} AS vehicle_key,
  v.vehicle_id,
  v.vehicle_name,
  v.model,
  v.manufacturer,
  
  -- Use NULL for all numeric fields
  NULL::NUMERIC AS cost_in_credits,
  'Unknown' AS cost_formatted,
  NULL::NUMERIC AS length_m,
  NULL::NUMERIC AS max_speed,
  'Unknown' AS speed_class,
  NULL::NUMERIC AS crew_count,
  NULL::NUMERIC AS passenger_capacity,
  NULL::NUMERIC AS total_capacity,
  NULL::NUMERIC AS cargo_capacity,
  
  -- Technical classifications
  v.vehicle_class,
  v.vehicle_size,
  v.vehicle_purpose,
  v.terrain_capability,
  
  -- Film and pilot information
  v.film_appearances,
  v.film_names,
  v.pilot_count,
  v.pilot_names,
  
  -- Star Wars universe context
  v.faction_affiliation,
  v.effectiveness_rating,
  
  -- Notable flag
  v.is_notable_vehicle AS is_iconic,
  
  -- Source tracking
  v.url AS source_url,
  v.fetch_timestamp,
  v.processed_timestamp,
  CURRENT_TIMESTAMP AS dbt_loaded_at
  
FROM vehicle_context v
WHERE v.vehicle_id IS NOT NULL
ORDER BY v.faction_affiliation, v.effectiveness_rating DESC, v.vehicle_name