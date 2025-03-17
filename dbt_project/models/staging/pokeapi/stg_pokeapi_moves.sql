{{
  config(
    materialized = 'view'
  )
}}

/*
  Model: stg_pokeapi_moves
  Description: Standardized Pokémon move data from PokeAPI
*/

SELECT
    id,
    name,
    type,
    power,
    pp,
    accuracy,
    priority,
    damage_class,
    effect_text,
    effect_chance,
    generation_id,
    target,
    created_at,
    updated_at
FROM {{ source('pokeapi', 'moves') }}