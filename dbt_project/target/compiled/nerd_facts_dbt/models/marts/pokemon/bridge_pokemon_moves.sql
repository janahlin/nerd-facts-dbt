

/*
  Model: bridge_pokemon_moves
  Description: Bridge table connecting Pokémon to their learnable moves
  
  Notes:
  - Handles the many-to-many relationship between Pokémon and moves
  - Gets moves from raw data since staging doesn't have them
  - Calculates STAB (Same Type Attack Bonus) and signature move flags
  - Provides context on move learning methods and levels
*/

WITH pokemon_base AS (
    -- Get base Pokemon data from staging
    SELECT
        p.pokemon_id,
        p.pokemon_name,
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
        -- These fields need to be handled differently as they're not in staging
        NULL AS generation_number,
        (SELECT SUM((stat_data->>'base_stat')::integer)
         FROM (
             SELECT
                 pokemon_id,
                 jsonb_array_elements(
                     COALESCE(NULLIF(stats::text, 'null')::jsonb, '[]'::jsonb)
                 ) AS stat_data
             FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p2
             WHERE p2.pokemon_id = p.pokemon_id
         ) st) AS total_base_stats
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p
    WHERE p.pokemon_id IS NOT NULL
),

pokemon_moves_raw AS (
    -- Get moves directly from raw data
    SELECT
        id,
        moves::jsonb AS moves_json
    FROM raw.pokeapi_pokemon
    WHERE id IS NOT NULL
),

pokemon_moves AS (
    -- Extract move references from the raw data with improved error handling
    SELECT
        pb.pokemon_id,
        pb.pokemon_name,
        pb.primary_type,
        pb.secondary_type,
        pb.generation_number,
        pb.total_base_stats,
        -- Extract move details from the moves array
        move_data->'move'->>'name' AS move_name,
        move_data->'move'->>'type' AS move_type,
        -- Extract learning method from version group details
        (
            SELECT vgd->>'move_learn_method'
            FROM jsonb_array_elements(move_data->'version_group_details') AS vgd
            LIMIT 1
        ) AS learn_method,
        -- Extract level requirement
        (
            SELECT COALESCE((vgd->>'level_learned_at')::integer, 0)
            FROM jsonb_array_elements(move_data->'version_group_details') AS vgd
            LIMIT 1
        ) AS level_learned_at
    FROM pokemon_base pb
    JOIN pokemon_moves_raw pmr ON pb.pokemon_id = pmr.id
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(NULLIF(pmr.moves_json::text, 'null')::jsonb, '[]'::jsonb)
    ) AS move_data
    WHERE pb.pokemon_id IS NOT NULL
)

SELECT
    -- Wrap everything in a subquery to use derived columns
    move_data.*,
    
    -- Move priority classification using the now-available is_signature_move column
    CASE
        WHEN move_data.is_signature_move THEN 'Signature'
        WHEN move_data.has_stab AND move_data.level_learned_at <= 20 THEN 'Early STAB'
        WHEN move_data.has_stab THEN 'STAB'
        WHEN move_data.learn_method = 'machine' THEN 'TM/HM'
        WHEN move_data.learn_method = 'tutor' THEN 'Tutor'
        ELSE 'Standard'
    END AS move_priority,
    
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM (
    SELECT
        -- Primary key
        md5(cast(coalesce(cast(pm.pokemon_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pm.move_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pm.learn_method as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(pm.level_learned_at, 0) as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pokemon_move_id,
        
        -- Core identifiers
        pm.pokemon_id,
        pm.pokemon_name,
        pm.move_name,
        pm.move_type,
        pm.learn_method,
        pm.level_learned_at,
        
        -- Check if move type matches EITHER Pokemon type (STAB detection)
        CASE
            WHEN pm.move_type = pm.primary_type THEN TRUE
            WHEN pm.move_type = pm.secondary_type AND pm.secondary_type IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_stab,
        
        -- Calculate if this is a signature move
        CASE
            -- Name-based detection
            WHEN pm.move_name LIKE CONCAT('%', pm.pokemon_name, '%') THEN TRUE
            
            -- Starter Pokémon signature moves
            WHEN (pm.pokemon_name = 'pikachu' AND pm.move_name = 'volt-tackle') THEN TRUE
            WHEN (pm.pokemon_name = 'charizard' AND pm.move_name = 'blast-burn') THEN TRUE
            WHEN (pm.pokemon_name = 'blastoise' AND pm.move_name = 'hydro-cannon') THEN TRUE
            WHEN (pm.pokemon_name = 'venusaur' AND pm.move_name = 'frenzy-plant') THEN TRUE
            
            -- Legendary signature moves
            WHEN (pm.pokemon_name = 'mewtwo' AND pm.move_name = 'psystrike') THEN TRUE
            WHEN (pm.pokemon_name = 'lugia' AND pm.move_name = 'aeroblast') THEN TRUE
            WHEN (pm.pokemon_name = 'ho-oh' AND pm.move_name = 'sacred-fire') THEN TRUE
            WHEN (pm.pokemon_name = 'kyogre' AND pm.move_name = 'origin-pulse') THEN TRUE
            WHEN (pm.pokemon_name = 'groudon' AND pm.move_name = 'precipice-blades') THEN TRUE
            WHEN (pm.pokemon_name = 'rayquaza' AND pm.move_name = 'dragon-ascent') THEN TRUE
            WHEN (pm.pokemon_name = 'dialga' AND pm.move_name = 'roar-of-time') THEN TRUE
            WHEN (pm.pokemon_name = 'palkia' AND pm.move_name = 'spacial-rend') THEN TRUE
            WHEN (pm.pokemon_name = 'giratina' AND pm.move_name = 'shadow-force') THEN TRUE
            
            ELSE FALSE
        END AS is_signature_move,
        
        -- Moves learned at level 1 or by evolution are typically important
        CASE
            WHEN pm.level_learned_at = 1 OR pm.learn_method = 'evolution' THEN TRUE
            ELSE FALSE
        END AS is_natural_move,
        
        -- Enhanced learn method classification
        CASE
            WHEN pm.learn_method = 'level-up' THEN 'Level Up'
            WHEN pm.learn_method = 'machine' THEN 'TM/HM'
            WHEN pm.learn_method = 'egg' THEN 'Egg Move'
            WHEN pm.learn_method = 'tutor' THEN 'Move Tutor'
            WHEN pm.learn_method = 'evolution' THEN 'Evolution'
            WHEN pm.learn_method = 'form-change' THEN 'Form Change'
            ELSE 'Other'
        END AS learn_method_type
        
    FROM pokemon_moves pm
) move_data
ORDER BY move_data.pokemon_id, move_data.level_learned_at, move_data.is_signature_move DESC