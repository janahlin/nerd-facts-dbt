

/*
  Model: stg_pokeapi_moves
  Description: Standardizes Pokémon move data from the PokeAPI
  
  Notes:
  - Added safe type casting for numeric fields
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        name,
        

        -- Numeric fields
        CASE WHEN power~E'^[0-9]+$' THEN power ELSE NULL END AS power,
        CASE WHEN pp~E'^[0-9]+$' THEN pp ELSE NULL END AS pp,
        CASE WHEN accuracy~E'^[0-9]+$' THEN accuracy ELSE NULL END AS accuracy,
        CASE WHEN priority~E'^[0-9]+$' THEN priority ELSE NULL END AS priority,
        CASE WHEN effect_chance~E'^[0-9]+$' THEN effect_chance ELSE NULL END AS effect_chance,        

        -- JSON fields
        damage_class,
        contest_combos,
        contest_type,
        contest_effect,
        effect_changes,
        effect_entries,
        flavor_text_entries,
        generation,
        learned_by_pokemon,
        machines,
        meta,
        names,
        past_values,
        stat_changes,
        type,
        target
    FROM "nerd_facts"."raw"."pokeapi_moves"
    WHERE id IS NOT NULL
)



SELECT
    -- Primary identifiers
    id as move_id,

    -- Text fields
    name as move_name,
    

    -- Numeric fields
    CAST(power AS NUMERIC) AS power,
    CAST(pp AS NUMERIC) AS pp,
    CAST(accuracy AS NUMERIC) AS accuracy,
    CAST(priority AS NUMERIC) AS priority,
    CAST(effect_chance AS NUMERIC) AS effect_chance,

    -- JSON fields
    damage_class,
    contest_combos,
    contest_type,
    contest_effect,
    effect_changes,
    effect_entries,
    flavor_text_entries,
    generation,
    learned_by_pokemon,
    machines,
    meta,
    names,
    past_values,
    stat_changes,
    type,
    target,
    -- Source tracking - removed missing source fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data