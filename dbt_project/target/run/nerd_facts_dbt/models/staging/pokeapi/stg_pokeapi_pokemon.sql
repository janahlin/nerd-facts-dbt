
  create view "nerd_facts"."public"."stg_pokeapi_pokemon__dbt_tmp"
    
    
  as (
    /*
  Model: stg_pokeapi_pokemon
  Description: Standardizes Pokémon data from the PokeAPI
  Source: raw.pokeapi_pokemon
  
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        name,
        is_default,

        -- Numeric fields
        CASE WHEN height~E'^[0-9]+$' THEN height ELSE NULL END AS height,
        CASE WHEN weight~E'^[0-9]+$' THEN weight ELSE NULL END AS weight,
        CASE WHEN bmi~E'^[0-9]+[.][0-9]+$' THEN bmi ELSE NULL END AS bmi,
        CASE WHEN base_experience~E'^[0-9]+$' THEN base_experience ELSE NULL END AS base_experience,
        CASE WHEN "order"~E'^[0-9]+$' THEN "order" ELSE NULL END AS pokemon_order,        

        -- JSON fields
        abilities,
        cries,
        forms,
        game_indices,
        held_items,
        location_area_encounters,
        moves,
        past_abilities,
        past_types,      
        stats,                
        species,
        sprites,
        types        
    FROM raw.pokeapi_pokemon
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id as pokemon_id,

    -- Text fields
    name as pokemon_name,
    is_default,

    -- Numeric fields
    CAST(height AS NUMERIC) AS height,
    CAST(weight AS NUMERIC) AS weight,
    CAST(bmi AS NUMERIC) AS bmi,
    CAST(base_experience AS NUMERIC) AS base_experience,
    CAST(pokemon_order AS NUMERIC) AS pokemon_order,

    -- JSON fields
    abilities,
    cries,
    forms,
    game_indices,
    held_items,
    location_area_encounters,
    moves,
    past_abilities,
    past_types,      
    stats,                
    species,
    sprites,
    types,

    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
  );