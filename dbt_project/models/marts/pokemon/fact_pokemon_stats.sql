{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['pokemon_id']}, {'columns': ['stat_class']}],
    unique_key = 'pokemon_stats_key'
  )
}}

/*
  Model: fact_pokemon_stats
  Description: Fact table for Pokémon statistics and battle metrics
  
  Notes:
  - Contains comprehensive stat analysis for all Pokémon
  - Calculates evolutionary stage and progression metrics
  - Includes battle effectiveness calculations and classifications
  - Provides stat distribution analysis and percentile rankings
  - Links to dimension tables for Pokémon and types
*/

WITH pokemon_types AS (
    SELECT
        pokemon_id,
        (jsonb_array_elements(
            COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
        )->'type')::jsonb->>'name' AS type_name,
        (jsonb_array_elements(
            COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
        )->>'slot')::int AS type_slot
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE pokemon_id IS NOT NULL
),

stats_extract AS (
    SELECT
        pokemon_id,
        jsonb_array_elements(
            COALESCE(NULLIF(stats::text, 'null')::jsonb, '[]'::jsonb)
        ) AS stat_data
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE pokemon_id IS NOT NULL
),

pokemon_stats AS (
    SELECT
        pokemon_id,
        stat_name,
        base_stat_value
    FROM (
        SELECT
            p.pokemon_id,
            jsonb_extract_path_text(s.stat_json, 'stat', 'name') AS stat_name,
            jsonb_extract_path_text(s.stat_json, 'base_stat')::integer AS base_stat_value
        FROM {{ ref('stg_pokeapi_pokemon') }} p
        CROSS JOIN LATERAL (
            SELECT jsonb_array_elements(
                COALESCE(NULLIF(p.stats::text, 'null')::jsonb, '[]'::jsonb)
            ) AS stat_json
        ) s
    ) extracted_stats
),

stats_aggregated AS (
    SELECT
        pokemon_id,
        MAX(CASE WHEN stat_name = 'hp' THEN base_stat_value ELSE 0 END) AS base_hp,
        MAX(CASE WHEN stat_name = 'attack' THEN base_stat_value ELSE 0 END) AS base_attack,
        MAX(CASE WHEN stat_name = 'defense' THEN base_stat_value ELSE 0 END) AS base_defense,
        MAX(CASE WHEN stat_name = 'special-attack' THEN base_stat_value ELSE 0 END) AS base_special_attack,
        MAX(CASE WHEN stat_name = 'special-defense' THEN base_stat_value ELSE 0 END) AS base_special_defense,
        MAX(CASE WHEN stat_name = 'speed' THEN base_stat_value ELSE 0 END) AS base_speed,
        SUM(base_stat_value) AS total_base_stats
    FROM pokemon_stats
    GROUP BY pokemon_id
),

species_data AS (
    SELECT
        pokemon_id,
        jsonb_extract_path_text(species, 'url') AS species_url,
        COALESCE(jsonb_extract_path_text(species, 'is_legendary')::boolean, false) AS is_legendary,
        COALESCE(jsonb_extract_path_text(species, 'is_mythical')::boolean, false) AS is_mythical
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE pokemon_id IS NOT NULL
),

pokemon_base AS (
    SELECT
        p.pokemon_id,
        p.pokemon_name,
        
        -- Get primary and secondary types from the slots
        (SELECT pt.type_name
         FROM pokemon_types pt
         WHERE pt.pokemon_id = p.pokemon_id AND pt.type_slot = 1
         LIMIT 1) AS primary_type,
         
        (SELECT pt.type_name
         FROM pokemon_types pt
         WHERE pt.pokemon_id = p.pokemon_id AND pt.type_slot = 2
         LIMIT 1) AS secondary_type,
        
        ps.base_hp,
        ps.base_attack,
        ps.base_defense,
        ps.base_special_attack,
        ps.base_special_defense,
        ps.base_speed,
        ps.total_base_stats,
        sp.is_legendary,
        sp.is_mythical
    FROM {{ ref('stg_pokeapi_pokemon') }} p
    LEFT JOIN stats_aggregated ps ON p.pokemon_id = ps.pokemon_id
    LEFT JOIN species_data sp ON p.pokemon_id = sp.pokemon_id
    WHERE p.pokemon_id IS NOT NULL
)

SELECT
    -- Primary key for this fact table
    {{ dbt_utils.generate_surrogate_key(['pb.pokemon_id']) }} AS pokemon_stats_key,
    
    -- Foreign keys
    pb.pokemon_id,
    pb.pokemon_name,
    pb.primary_type,
    pb.secondary_type,
    
    -- Base stats
    pb.base_hp,
    pb.base_attack,
    pb.base_defense,
    pb.base_special_attack,
    pb.base_special_defense,
    pb.base_speed,
    pb.total_base_stats,
    
    -- Calculated stat metrics
    ROUND((pb.base_hp + pb.base_defense + pb.base_special_defense) / 3.0, 1) AS defensive_average,
    ROUND((pb.base_attack + pb.base_special_attack + pb.base_speed) / 3.0, 1) AS offensive_average,
    
    -- Stat classification
    CASE
        WHEN pb.total_base_stats >= 580 THEN 'Elite'
        WHEN pb.total_base_stats >= 500 THEN 'Strong'
        WHEN pb.total_base_stats >= 420 THEN 'Average'
        ELSE 'Basic'
    END AS stat_class,
    
    -- Battle focus based on stats
    CASE
        WHEN (pb.base_attack + pb.base_special_attack) > 
             (pb.base_defense + pb.base_special_defense) + 20 THEN 'Offensive'
        WHEN (pb.base_defense + pb.base_special_defense) > 
             (pb.base_attack + pb.base_special_attack) + 20 THEN 'Defensive'
        ELSE 'Balanced'
    END AS battle_focus,
    
    -- Attack preference
    CASE
        WHEN pb.base_attack > pb.base_special_attack + 20 THEN 'Physical'
        WHEN pb.base_special_attack > pb.base_attack + 20 THEN 'Special'
        ELSE 'Mixed'
    END AS attack_preference,
    
    -- Special status
    pb.is_legendary,
    pb.is_mythical,
    
    -- Meta field for tracking changes
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM pokemon_base pb
WHERE pb.pokemon_id IS NOT NULL
ORDER BY pb.total_base_stats DESC 