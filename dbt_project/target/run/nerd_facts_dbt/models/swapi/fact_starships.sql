
  create view "nerd_facts"."public"."fact_starships__dbt_tmp"
    
    
  as (
    

SELECT
    id,
    name,
    model,
    manufacturer,
    cost_in_credits::BIGINT AS cost_in_credits,
    length::DECIMAL AS length,
    -- ✅ Convert `max_atmosphering_speed` safely
    CASE
        WHEN max_atmosphering_speed ~ '^\d+$' THEN max_atmosphering_speed::INTEGER
        ELSE NULL
    END AS max_atmosphering_speed,  
    crew::INTEGER AS crew,
    passengers::INTEGER AS passengers,
    cargo_capacity::BIGINT AS cargo_capacity,
    consumables,
    hyperdrive_rating::DECIMAL AS hyperdrive_rating,
    MGLT::INTEGER AS MGLT,
    starship_class,
    pilots::JSONB AS pilot_ids, -- JSONB for linked pilots
    url
FROM "nerd_facts"."raw"."swapi_starships"
  );