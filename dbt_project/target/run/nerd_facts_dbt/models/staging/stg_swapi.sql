
  create view "nerd_facts"."public"."stg_swapi__dbt_tmp"
    
    
  as (
    WITH raw_data AS (
    SELECT * FROM raw.swapi_starships
)
SELECT
    id,
    name AS starship_name,
    model,
    manufacturer,
    
    -- ✅ Extract first number, handle "unknown" values safely
    CASE 
        WHEN lower(max_atmosphering_speed) = 'unknown' THEN NULL  -- ✅ Convert "unknown" to NULL
        ELSE NULLIF(REGEXP_REPLACE(max_atmosphering_speed, '[^0-9]+.*', '', 'g'), '')::INTEGER 
    END AS max_speed,
    
    -- ✅ Convert crew and passengers safely
    NULLIF(REGEXP_REPLACE(crew, '[^0-9]', '', 'g'), '')::INTEGER AS crew,  
    NULLIF(REGEXP_REPLACE(passengers, '[^0-9]', '', 'g'), '')::INTEGER AS passengers  

FROM raw_data
  );