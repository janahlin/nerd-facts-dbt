/*
  Model: stg_pokeapi_items
  Description: Standardizes Pokémon item data from the PokeAPI
  Source: raw.pokeapi_items
  
  Notes:  
  - Fixed type casting issues with numeric fields
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        name,        

        -- Numeric fields
        CASE WHEN cost ~ E'^[0-9]+$' THEN cost ELSE NULL END AS cost,
        CASE WHEN fling_power ~ E'^[0-9]+$' THEN fling_power ELSE NULL END AS fling_power,

        -- JSON fields
        attributes,
        baby_trigger_for,
        category,
        effect_entries, 
        flavor_text_entries,       
        fling_effect,
        game_indices,
        held_by_pokemon,
        names,
        machines,        
        sprites
    FROM raw.pokeapi_items
    WHERE id IS NOT NULL
)

SELECT
         -- Primary identifiers
        id as item_id,

        -- Text fields
        name as item_name,        

        -- Numeric fields
        CAST(cost AS NUMERIC) AS cost,
        CAST(fling_power AS NUMERIC) AS fling_power,        

        -- JSON fields
        attributes,
        baby_trigger_for,
        category,
        effect_entries, 
        flavor_text_entries,       
        fling_effect,
        game_indices,
        held_by_pokemon,
        names,
        machines,        
        sprites,
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at    
FROM raw_data 
