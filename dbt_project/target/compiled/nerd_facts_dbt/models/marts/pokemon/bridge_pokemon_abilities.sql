

/*
  Model: bridge_pokemon_abilities
  Description: Bridge table connecting Pokémon to their abilities
  
  Notes:
  - Handles the many-to-many relationship between Pokémon and abilities
  - Modified to access abilities directly from the source table
  - Calculates synergy scores between Pokémon and their abilities
  - Provides context on whether abilities are hidden or standard
  - Removed dimension table join until dim_pokemon_abilities is created
*/

-- First get the core Pokémon data we need
WITH pokemon_data AS (
    SELECT
        p.pokemon_id,
        p.pokemon_name,
        -- Extract primary type
        (SELECT jsonb_extract_path_text(pt.type_json::jsonb, 'name')
         FROM (
             SELECT
                 pokemon_id,
                 jsonb_array_elements(
                     COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
                 )->>'type' AS type_json,
                 (jsonb_array_elements(
                     COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
                 )->'slot')::int AS type_slot
             FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
             WHERE p2.pokemon_id = p.pokemon_id
         ) pt
         WHERE pt.type_slot = 1
         LIMIT 1) AS primary_type,
        -- Extract secondary type
        (SELECT jsonb_extract_path_text(pt.type_json::jsonb, 'name')
         FROM (
             SELECT
                 pokemon_id,
                 jsonb_array_elements(
                     COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
                 )->>'type' AS type_json,
                 (jsonb_array_elements(
                     COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
                 )->'slot')::int AS type_slot
             FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
             WHERE p2.pokemon_id = p.pokemon_id
         ) pt
         WHERE pt.type_slot = 2
         LIMIT 1) AS secondary_type,
        -- Extract species data for generation, legendary, mythical
        (SELECT 
            CASE 
                WHEN gen.gen_num IS NOT NULL THEN gen.gen_num::integer
                ELSE NULL
            END
         FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
         LEFT JOIN LATERAL (
             SELECT (regexp_matches(jsonb_extract_path_text(species, 'url'), '/generation/([0-9]+)/'))[1] AS gen_num
             WHERE jsonb_extract_path_text(species, 'url') ~ '/generation/([0-9]+)/'
         ) gen ON true
         WHERE p2.pokemon_id = p.pokemon_id) AS generation_number,
        (SELECT COALESCE(jsonb_extract_path_text(species, 'is_legendary')::boolean, false)
         FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
         WHERE p2.pokemon_id = p.pokemon_id) AS is_legendary,
        (SELECT COALESCE(jsonb_extract_path_text(species, 'is_mythical')::boolean, false)
         FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
         WHERE p2.pokemon_id = p.pokemon_id) AS is_mythical,
        -- Calculate total base stats
        (SELECT SUM((stat_data->>'base_stat')::integer)
         FROM (
             SELECT
                 pokemon_id,
                 jsonb_array_elements(
                     COALESCE(NULLIF(stats::text, 'null')::jsonb, '[]'::jsonb)
                 ) AS stat_data
             FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
             WHERE p2.pokemon_id = p.pokemon_id
         ) st) AS total_base_stats,
        -- Count abilities
        (SELECT COUNT(*)
         FROM (
             SELECT
                 jsonb_array_elements(
                     COALESCE(NULLIF(abilities::text, 'null')::jsonb, '[]'::jsonb)
                 ) AS ability_data
             FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
             WHERE p2.pokemon_id = p.pokemon_id
         ) ab) AS ability_count
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p
    WHERE p.pokemon_id IS NOT NULL
),

-- Access raw data directly to get the abilities
pokemon_abilities_raw AS (
    SELECT
        id,
        abilities::jsonb AS abilities_json
    FROM raw.pokeapi_pokemon
    WHERE id IS NOT NULL
),

-- Extract ability references from the raw data
pokemon_abilities AS (
    SELECT
        pd.pokemon_id,
        pd.pokemon_name,
        pd.primary_type,
        pd.secondary_type,
        pd.generation_number,
        pd.is_legendary,
        pd.is_mythical,
        pd.total_base_stats,
        ability_ref->'ability'->>'name' AS ability_name,
        (ability_ref->>'is_hidden')::boolean AS is_hidden,
        (ability_ref->>'slot')::integer AS slot_number
    FROM pokemon_data pd
    JOIN pokemon_abilities_raw par ON pd.pokemon_id = par.id
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(NULLIF(par.abilities_json::text, 'null')::jsonb, '[]'::jsonb)
    ) AS ability_ref
    WHERE pd.pokemon_id IS NOT NULL
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(pa.pokemon_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pa.ability_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pokemon_ability_id,
    
    -- Foreign keys
    pa.pokemon_id,
    pa.pokemon_name,
    pa.ability_name,
    
    -- Ability attributes
    pa.is_hidden,
    pa.slot_number,
    
    -- Pokemon attributes for context
    pa.primary_type,
    pa.secondary_type,
    pa.generation_number,
    pa.is_legendary,
    pa.is_mythical,
    
    -- Synergy rating between Pokemon and ability
    CASE
        -- Top competitive synergies (examples from competitive play)
        WHEN (pa.pokemon_name = 'gengar' AND pa.ability_name = 'levitate') THEN 5
        WHEN (pa.pokemon_name = 'gyarados' AND pa.ability_name = 'intimidate') THEN 5
        WHEN (pa.pokemon_name = 'garchomp' AND pa.ability_name = 'rough-skin') THEN 5
        WHEN (pa.pokemon_name = 'dragonite' AND pa.ability_name = 'multiscale') THEN 5
        
        -- Weather abilities synergy with types
        WHEN (pa.ability_name LIKE '%drought%' AND pa.primary_type = 'fire') THEN 5
        WHEN (pa.ability_name LIKE '%drizzle%' AND pa.primary_type = 'water') THEN 5
        WHEN (pa.ability_name LIKE '%sand-stream%' AND pa.primary_type IN ('rock', 'ground')) THEN 5
        WHEN (pa.ability_name LIKE '%snow-warning%' AND pa.primary_type = 'ice') THEN 5
        
        -- Type boosting abilities
        WHEN (pa.ability_name LIKE '%blaze%' AND pa.primary_type = 'fire') THEN 4
        WHEN (pa.ability_name LIKE '%torrent%' AND pa.primary_type = 'water') THEN 4
        WHEN (pa.ability_name LIKE '%overgrow%' AND pa.primary_type = 'grass') THEN 4
        
        -- Legendary/Mythical synergies are often designed to be good
        WHEN (pa.is_legendary OR pa.is_mythical) AND pa.is_hidden = FALSE THEN 4.5
        
        -- Hidden abilities are often better (especially in later generations)
        WHEN pa.is_hidden AND pa.generation_number >= 5 THEN 4
        WHEN pa.is_hidden THEN 3.5
        
        -- Default synergy (still decent)
        ELSE 3
    END AS ability_synergy,
    
    -- Competitive relevance indicator (simplified)
    CASE
        WHEN pa.ability_name IN (
            'speed-boost', 'protean', 'intimidate', 'drought', 'drizzle', 
            'sand-stream', 'adaptability', 'huge-power', 'multiscale'
        ) THEN TRUE
        WHEN (pa.is_legendary OR pa.is_mythical) AND pa.is_hidden THEN TRUE
        ELSE FALSE
    END AS is_competitively_relevant,
    
    -- Generation relationship
    CASE 
        WHEN pa.ability_name IN ('intimidate', 'levitate', 'chlorophyll', 
                              'swift-swim', 'sand-stream', 'drought', 'drizzle') 
             AND pa.generation_number <= 3 THEN 'Original Ability'
        WHEN pa.generation_number >= 6 AND pa.is_hidden THEN 'Modern Hidden Ability'
        WHEN pa.is_hidden THEN 'Hidden Ability'
        ELSE 'Standard Ability'
    END AS ability_classification,
    
    -- Add data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM pokemon_abilities pa
ORDER BY pa.pokemon_id, pa.slot_number