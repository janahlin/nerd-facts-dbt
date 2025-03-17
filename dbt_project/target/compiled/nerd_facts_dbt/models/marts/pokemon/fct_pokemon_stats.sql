

/*
  Model: fct_pokemon_stats
  Description: Fact table for Pokémon statistics and battle metrics
  
  Notes:
  - Contains comprehensive stat analysis for all Pokémon
  - Calculates evolutionary stage and progression metrics
  - Includes battle effectiveness calculations and classifications
  - Provides stat distribution analysis and percentile rankings
  - Links to dimension tables for Pokémon and types
*/

WITH base_pokemon AS (
    SELECT
        -- Core identifiers
        id,
        name,
        type_list[0] AS primary_type,
        type_list[1] AS secondary_type,
        generation_number,
        is_legendary,
        is_mythical,
        
        -- Physical attributes with unit conversions
        height_dm / 10.0 AS height_m,
        weight_kg,
        base_xp,
        
        -- Base stats - using exact column names from staging
        base_stat_hp,
        total_base_stats
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE id IS NOT NULL
),

-- Calculate stat percentiles across all Pokémon
stat_percentiles AS (
    SELECT
        id,
        PERCENT_RANK() OVER (ORDER BY base_stat_hp) AS hp_percentile,
        PERCENT_RANK() OVER (ORDER BY total_base_stats) AS total_stats_percentile
    FROM base_pokemon
),

-- Improved evolutionary stage determination with simpler logic
evolution_stage AS (
    SELECT
        bp.id,
        CASE
            WHEN bp.is_legendary OR bp.is_mythical THEN 3  -- Legendaries are final forms
            WHEN bp.total_base_stats < 350 THEN 1          -- Basic form
            WHEN bp.total_base_stats < 470 THEN 2          -- First evolution
            ELSE 3                                         -- Final evolution
        END AS evolution_level
    FROM base_pokemon bp
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(bp.id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pokemon_stat_id,
    
    -- Core identifiers
    bp.id AS pokemon_id,
    bp.name AS pokemon_name,
    bp.primary_type,
    bp.secondary_type,
    
    -- Physical attributes
    bp.height_m AS height,
    bp.weight_kg AS weight,
    bp.base_xp,
    
    -- Stats
    bp.base_stat_hp,
    bp.total_base_stats,
    
    -- Percentiles
    ROUND(sp.hp_percentile * 100) AS hp_percentile,
    ROUND(sp.total_stats_percentile * 100) AS total_stats_percentile,
    
    -- Evolution data
    es.evolution_level,
    
    -- Additional attributes
    bp.generation_number,
    bp.is_legendary,
    bp.is_mythical,
    
    -- Metadata
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM base_pokemon bp
JOIN stat_percentiles sp ON bp.id = sp.id
JOIN evolution_stage es ON bp.id = es.id
ORDER BY bp.total_base_stats DESC