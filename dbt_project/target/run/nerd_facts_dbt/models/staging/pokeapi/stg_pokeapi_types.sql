
  create view "nerd_facts"."public"."stg_pokeapi_types__dbt_tmp"
    
    
  as (
    /*
  Model: stg_pokeapi_types
  Description: Standardizes Pokémon type data from the PokeAPI
  Source: raw.pokeapi_types
  
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        name,

        -- JSON fields
        damage_relations,
        game_indices,
        generation,
        move_damage_class,
        moves,
        names,
        past_damage_relations,
        pokemon,
        sprites
    FROM raw.pokeapi_types
    WHERE id IS NOT NULL
)


SELECT
    -- Primary identifiers
    id AS type_id,

    -- Text fields
    name AS type_name,

    -- JSON fields
    damage_relations,
    game_indices,
    generation,
    move_damage_class,
    moves,
    names,
    past_damage_relations,
    pokemon,
    sprites,
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
  );