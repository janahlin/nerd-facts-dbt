
  create view "nerd_facts"."public"."stg_swapi_starships__dbt_tmp"
    
    
  as (
    

SELECT
    id,
    name,
    model,
    manufacturer,

    -- ✅ Use NULLIF to avoid errors if value is empty ('')
    NULLIF(cost_in_credits, '')::BIGINT AS cost_in_credits,
    NULLIF(length, '')::DECIMAL AS length,
    NULLIF(max_atmosphering_speed, '')::INTEGER AS max_atmosphering_speed,
    NULLIF(crew, '')::INTEGER AS crew,
    NULLIF(passengers, '')::INTEGER AS passengers,
    NULLIF(cargo_capacity, '')::BIGINT AS cargo_capacity,
    
    consumables,

    NULLIF(hyperdrive_rating, '')::DECIMAL AS hyperdrive_rating,
    NULLIF(MGLT, '')::INTEGER AS MGLT,
    
    starship_class,

    -- ✅ Ensure pilots column is always valid JSONB
    CASE 
        WHEN pilots IS NULL OR pilots = '' THEN '[]'::JSONB
        ELSE pilots::JSONB
    END AS pilot_ids,

    url

FROM "nerd_facts"."raw"."swapi_starships"
  );