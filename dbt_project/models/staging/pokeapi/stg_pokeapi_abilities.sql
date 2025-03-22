/*
  Model: stg_pokeapi_abilities
  Description: Standardizes Pokémon ability data from the PokeAPI
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        name,
        is_main_series,

        -- Generation information with explicit JSONB casting
        flavor_text_entries,
        effect_entries,
        effect_changes,
        names,
        generation, 
        pokemon
    FROM raw.pokeapi_abilities
    WHERE id IS NOT NULL
)


SELECT
    -- Primary identifiers
    id as ability_id,
    name AS ability_name,

    -- Text fields  
    is_main_series,

    -- Generation information with explicit JSONB casting    
    flavor_text_entries,
    effect_entries,
    effect_changes,
    names,
    generation,    
    pokemon,    

    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data