{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['pokemon_id']}, {'columns': ['primary_type', 'secondary_type']}],
    unique_key = 'pokemon_key'
  )
}}

/*
  Model: fct_pokemon
  Description: Core fact table for Pokémon data with comprehensive attributes and classifications
  
  Notes:
  - Contains essential information about each Pokémon species
  - Links to all related dimension tables (types, abilities, moves, etc.)
  - Provides both source fields and derived/calculated metrics
  - Includes generation, evolutionary, and classification data
  - Serves as the central entity for all Pokémon analysis
*/

WITH base_pokemon AS (
    SELECT
        -- Core identifiers
        id AS pokemon_id,
        name AS pokemon_name,
        type_list[0] AS primary_type,
        type_list[1] AS secondary_type,
        generation_number,
        is_legendary,
        is_mythical,
        
        -- Physical attributes
        height_dm,
        weight_kg,
        base_xp,
        
        -- Base stats - only use what exists in staging
        base_stat_hp,
        total_base_stats
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE id IS NOT NULL
)

SELECT
    -- Primary key and identifiers
    {{ dbt_utils.generate_surrogate_key(['bp.pokemon_id']) }} AS pokemon_key,
    bp.pokemon_id,
    
    -- Core data
    bp.pokemon_name,
    bp.primary_type,
    bp.secondary_type,
    
    -- Physical attributes
    bp.height_dm / 10.0 AS height_m,
    bp.weight_kg,
    bp.base_xp,
    
    -- Stats
    bp.base_stat_hp,
    bp.total_base_stats,
    
    -- Type keys
    {{ dbt_utils.generate_surrogate_key(['bp.primary_type']) }} AS primary_type_key,
    {{ dbt_utils.generate_surrogate_key(['bp.secondary_type']) }} AS secondary_type_key,
    
    -- Additional attributes
    bp.generation_number,
    bp.is_legendary,
    bp.is_mythical,
    
    -- Meta
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM base_pokemon bp
WHERE bp.pokemon_id IS NOT NULL
ORDER BY bp.total_base_stats DESC