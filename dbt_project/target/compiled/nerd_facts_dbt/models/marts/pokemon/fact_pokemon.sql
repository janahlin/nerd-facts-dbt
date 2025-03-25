

/*
  Model: fact_pokemon
  Description: Core fact table for Pokémon data with comprehensive attributes and classifications
  
  Notes:
  - Contains essential information about each Pokémon species
  - Links to all related dimension tables (types, abilities, moves, etc.)
  - Provides both source fields and derived/calculated metrics
  - Includes generation, evolutionary, and classification data
  - Serves as the central entity for all Pokémon analysis
*/

WITH pokemon_types AS (
    SELECT
        pokemon_id,
        jsonb_array_elements(
            COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
        )->>'type' AS type_json,
        (jsonb_array_elements(
            COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
        )->'slot')::int AS type_slot
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE pokemon_id IS NOT NULL
),

stats_extract AS (
    SELECT
        pokemon_id,
        jsonb_array_elements(
            COALESCE(NULLIF(stats::text, 'null')::jsonb, '[]'::jsonb)
        ) AS stat_data
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE pokemon_id IS NOT NULL
),

pokemon_stats AS (
    SELECT
        pokemon_id,
        SUM(CASE WHEN (stat_data->'stat'->>'name') = 'hp' 
                 THEN (stat_data->>'base_stat')::integer ELSE 0 END) AS base_stat_hp,
        SUM((stat_data->>'base_stat')::integer) AS total_base_stats
    FROM stats_extract
    GROUP BY pokemon_id
),

species_data AS (
    SELECT
        pokemon_id,
        jsonb_extract_path_text(species, 'url') AS species_url,
        -- Extract generation number from the URL using LATERAL join
        (SELECT gen.gen_num::integer
         FROM LATERAL (
             SELECT (regexp_matches(jsonb_extract_path_text(species, 'url'), '/generation/([0-9]+)/'))[1] AS gen_num
             WHERE jsonb_extract_path_text(species, 'url') ~ '/generation/([0-9]+)/'
         ) gen) AS generation_number,
        COALESCE(jsonb_extract_path_text(species, 'is_legendary')::boolean, false) AS is_legendary,
        COALESCE(jsonb_extract_path_text(species, 'is_mythical')::boolean, false) AS is_mythical
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE pokemon_id IS NOT NULL
),

base_pokemon AS (
    SELECT
        -- Core identifiers
        p.pokemon_id,
        p.pokemon_name,
        
        -- Get primary and secondary types from the slots
        (SELECT jsonb_extract_path_text(pt.type_json::jsonb, 'name')
         FROM pokemon_types pt
         WHERE pt.pokemon_id = p.pokemon_id AND pt.type_slot = 1
         LIMIT 1) AS primary_type,
         
        (SELECT jsonb_extract_path_text(pt.type_json::jsonb, 'name')
         FROM pokemon_types pt
         WHERE pt.pokemon_id = p.pokemon_id AND pt.type_slot = 2
         LIMIT 1) AS secondary_type,
         
        sp.generation_number,
        sp.is_legendary,
        sp.is_mythical,
        
        -- Physical attributes
        p.height / 10.0 AS height_dm,
        p.weight / 10.0 AS weight_kg,
        p.base_experience AS base_xp,
        
        -- Base stats
        ps.base_stat_hp,
        ps.total_base_stats
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p
    LEFT JOIN pokemon_stats ps ON p.pokemon_id = ps.pokemon_id
    LEFT JOIN species_data sp ON p.pokemon_id = sp.pokemon_id
    WHERE p.pokemon_id IS NOT NULL
)

SELECT
    -- Primary key and identifiers
    md5(cast(coalesce(cast(bp.pokemon_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pokemon_key,
    bp.pokemon_id,
    
    -- Core data
    bp.pokemon_name,
    bp.primary_type,
    bp.secondary_type,
    
    -- Physical attributes
    bp.height_dm AS height_m,
    bp.weight_kg,
    bp.base_xp,
    
    -- Stats
    bp.base_stat_hp,
    bp.total_base_stats,
    
    -- Type keys
    md5(cast(coalesce(cast(bp.primary_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS primary_type_key,
    md5(cast(coalesce(cast(bp.secondary_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS secondary_type_key,
    
    -- Additional attributes
    bp.generation_number,
    bp.is_legendary,
    bp.is_mythical,
    
    -- Meta
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM base_pokemon bp
WHERE bp.pokemon_id IS NOT NULL
ORDER BY bp.total_base_stats DESC