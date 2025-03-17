
  create view "nerd_facts"."public"."stg_pokeapi__dbt_tmp"
    
    
  as (
    WITH raw_data AS (
    SELECT * FROM raw.pokeapi_pokemon
)
SELECT
    id,
    name AS pokemon_name,
    height::NUMERIC / 10.0 AS height_m,
    weight::NUMERIC / 10.0 AS weight_kg,
    base_experience,
    jsonb_array_length(abilities::JSONB) AS num_abilities,  -- ✅ Cast abilities to JSONB
    abilities::JSONB AS abilities_json,  -- ✅ Ensure abilities are stored as JSONB
    (types::JSONB)->0->'type'->>'name' AS primary_type  -- ✅ Ensure types is JSONB before extracting
FROM raw_data
  );