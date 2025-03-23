

-- Get vehicle data from intermediate layer
WITH vehicle_film_data AS (
  SELECT
    fv.vehicle_id,
    COUNT(DISTINCT fv.film_id) AS film_count,
    STRING_AGG(f.title, ', ' ORDER BY f.episode_id) AS film_appearances
  FROM "nerd_facts"."public"."int_swapi_films_vehicles" fv
  JOIN "nerd_facts"."public"."int_swapi_films" f ON fv.film_id = f.film_id
  GROUP BY fv.vehicle_id
),

-- Add context data without trying numeric conversions
vehicle_context AS (
  SELECT
    v.vehicle_id,
    v.vehicle_name,  -- Already using vehicle_name
    v.model,
    v.manufacturer,
    v.vehicle_class,
    v.consumables,
    COALESCE(vfd.film_count, 0) AS film_appearances_count,
    COALESCE(vfd.film_appearances, 'None') AS film_appearances,
    
    -- Size classification based on known vehicle classes
    CASE
      WHEN v.vehicle_class ILIKE '%walker%' THEN 'Very Large'
      WHEN v.vehicle_class ILIKE '%transport%' THEN 'Large'
      WHEN v.vehicle_class ILIKE '%speeder%' THEN 'Medium'
      WHEN v.vehicle_class ILIKE '%bike%' OR v.vehicle_class ILIKE '%pod%' THEN 'Small'
      ELSE 'Medium'
    END AS vehicle_size,
    
    -- Purpose classification
    CASE 
      WHEN v.vehicle_class ILIKE '%combat%' OR 
           v.vehicle_class ILIKE '%assault%' OR
           v.vehicle_class ILIKE '%walker%' OR
           v.vehicle_class ILIKE '%fighter%' THEN 'Military'
      WHEN v.vehicle_class ILIKE '%transport%' OR
           v.vehicle_class ILIKE '%cargo%' THEN 'Transport'
      WHEN v.vehicle_class ILIKE '%speeder%' THEN 'Civilian'
      ELSE 'Multipurpose'
    END AS vehicle_purpose,
    
    -- Terrain capability
    CASE
      WHEN v.vehicle_class ILIKE '%speeder%' AND v.vehicle_class ILIKE '%snow%' THEN 'Snow'
      WHEN v.vehicle_class ILIKE '%speeder%' THEN 'Ground'
      WHEN v.vehicle_class ILIKE '%submarine%' THEN 'Water'
      WHEN v.vehicle_class ILIKE '%walker%' THEN 'All-Terrain'
      WHEN v.vehicle_class ILIKE '%airspeeder%' THEN 'Air'
      ELSE 'Ground'
    END AS terrain_capability,
    
    -- Notable vehicles
    CASE 
      WHEN v.vehicle_name IN ('AT-AT', 'AT-ST', 'Snowspeeder', 'Speeder bike',
                     'Imperial Speeder Bike', 'Sand Crawler', 'TIE bomber',
                     'TIE fighter', 'X-34 landspeeder') THEN TRUE
      ELSE FALSE
    END AS is_notable_vehicle,
    
    -- Move faction affiliation calculation to the CTE so it's available for ORDER BY
    CASE
      WHEN LOWER(v.vehicle_name) LIKE '%imperial%' OR
           LOWER(v.vehicle_name) IN ('at-at', 'at-st', 'at-dp', 'tie bomber',
                                 'tie fighter', 'tie interceptor') THEN 'Imperial'
      WHEN LOWER(v.vehicle_name) LIKE '%republic%' THEN 'Republic'
      WHEN LOWER(v.vehicle_name) LIKE '%rebel%' OR
           LOWER(v.vehicle_name) IN ('snowspeeder', 'x-34 landspeeder') THEN 'Rebel Alliance'
      ELSE 'Civilian/Neutral'
    END AS faction_affiliation
    
  FROM "nerd_facts"."public"."int_swapi_vehicles" v
  LEFT JOIN vehicle_film_data vfd ON v.vehicle_id = vfd.vehicle_id
)

-- Final output with surrogate key and enriched attributes
SELECT 
  md5(cast(coalesce(cast(v.vehicle_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS vehicle_key,
  v.vehicle_id,
  v.vehicle_name,
  v.model,
  v.manufacturer,
  
  -- Technical classifications
  v.vehicle_class,
  v.vehicle_size,
  v.vehicle_purpose,
  v.terrain_capability,
  v.consumables,
  
  -- Film information
  v.film_appearances_count,
  v.film_appearances,
  
  -- Star Wars universe context - using the pre-calculated field
  v.faction_affiliation,
  
  -- Calculate effectiveness rating
  CASE 
    WHEN v.vehicle_purpose = 'Military' THEN
      GREATEST(1, LEAST(10, 5 + 
        CASE
          WHEN v.vehicle_size = 'Very Large' THEN 3
          WHEN v.vehicle_size = 'Large' THEN 2
          WHEN v.vehicle_size = 'Medium' THEN 1
          WHEN v.vehicle_size = 'Small' THEN 0
          ELSE 0
        END
      ))
    WHEN v.vehicle_purpose = 'Transport' THEN 5
    ELSE 3
  END AS effectiveness_rating,
  
  -- Notable flag
  v.is_notable_vehicle AS is_iconic,
  
  -- Time dimension
  CURRENT_TIMESTAMP AS dbt_loaded_at
  
FROM vehicle_context v
WHERE v.vehicle_id IS NOT NULL
ORDER BY v.faction_affiliation, v.vehicle_name