{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['pokemon_id']}, {'columns': ['move_name']}],
    unique_key = 'pokemon_move_id'
  )
}}

/*
  Model: bridge_pokemon_moves
  Description: Bridge table connecting Pokémon to their learnable moves
  
  Notes:
  - Handles the many-to-many relationship between Pokémon and moves
  - Extracts move references from nested JSON in the pokémon staging model
  - Calculates STAB (Same Type Attack Bonus) and signature move flags
  - Provides context on move learning methods and levels
  - Calculates move relevance for competitive analysis
*/

WITH pokemon_moves AS (
    -- Extract move references from the pokemon data with improved error handling
    SELECT
        p.id AS pokemon_id,
        p.name AS pokemon_name,
        p.primary_type,
        p.secondary_type,
        p.generation_number,
        p.total_base_stats,
        COALESCE(move_ref->>'name', 'Unknown') AS move_name,
        COALESCE(move_ref->>'type', 'Unknown') AS move_type,
        COALESCE((move_ref->>'learn_method')::text, 'level-up') AS learn_method,
        COALESCE((move_ref->>'level_learned_at')::integer, 0) AS level_learned_at
    FROM {{ ref('stg_pokeapi_pokemon') }} p,
    LATERAL jsonb_array_elements(
        CASE WHEN p.moves IS NULL OR p.moves = 'null' THEN '[]'::jsonb
        ELSE p.moves END
    ) AS move_ref
    WHERE p.id IS NOT NULL
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['pm.pokemon_id', 'pm.move_name', 'pm.learn_method', 'COALESCE(pm.level_learned_at, 0)']) }} AS pokemon_move_id,
    
    -- Foreign keys
    {{ dbt_utils.generate_surrogate_key(['pm.pokemon_id']) }} AS pokemon_key,
    {{ dbt_utils.generate_surrogate_key(['m.move_id']) }} AS move_key,
    
    -- Core identifiers
    pm.pokemon_id,
    pm.pokemon_name,
    pm.move_name,
    pm.move_type,
    pm.learn_method,
    pm.level_learned_at,
    
    -- Add move stats from the moves dimension
    COALESCE(m.move_id, 0) AS move_id,
    COALESCE(m.power, 0) AS move_power,
    COALESCE(m.accuracy, 0) AS move_accuracy,
    COALESCE(m.pp, 0) AS move_pp,
    COALESCE(m.damage_class, 'Unknown') AS damage_class,
    
    -- Check if move type matches EITHER Pokemon type (improved STAB detection)
    CASE
        WHEN pm.move_type = pm.primary_type THEN TRUE
        WHEN pm.move_type = pm.secondary_type AND pm.secondary_type IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_stab,
    
    -- Calculate if this is a signature move (significantly expanded)
    CASE
        -- Name-based detection
        WHEN pm.move_name LIKE CONCAT('%', pm.pokemon_name, '%') THEN TRUE
        
        -- Starter Pokémon signature moves
        WHEN (pm.pokemon_name = 'Pikachu' AND pm.move_name = 'Volt Tackle') THEN TRUE
        WHEN (pm.pokemon_name = 'Charizard' AND pm.move_name = 'Blast Burn') THEN TRUE
        WHEN (pm.pokemon_name = 'Blastoise' AND pm.move_name = 'Hydro Cannon') THEN TRUE
        WHEN (pm.pokemon_name = 'Venusaur' AND pm.move_name = 'Frenzy Plant') THEN TRUE
        
        -- Legendary signature moves
        WHEN (pm.pokemon_name = 'Mewtwo' AND pm.move_name = 'Psystrike') THEN TRUE
        WHEN (pm.pokemon_name = 'Lugia' AND pm.move_name = 'Aeroblast') THEN TRUE
        WHEN (pm.pokemon_name = 'Ho-Oh' AND pm.move_name = 'Sacred Fire') THEN TRUE
        WHEN (pm.pokemon_name = 'Kyogre' AND pm.move_name = 'Origin Pulse') THEN TRUE
        WHEN (pm.pokemon_name = 'Groudon' AND pm.move_name = 'Precipice Blades') THEN TRUE
        WHEN (pm.pokemon_name = 'Rayquaza' AND pm.move_name = 'Dragon Ascent') THEN TRUE
        WHEN (pm.pokemon_name = 'Dialga' AND pm.move_name = 'Roar of Time') THEN TRUE
        WHEN (pm.pokemon_name = 'Palkia' AND pm.move_name = 'Spacial Rend') THEN TRUE
        WHEN (pm.pokemon_name = 'Giratina' AND pm.move_name = 'Shadow Force') THEN TRUE
        WHEN (pm.pokemon_name = 'Zekrom' AND pm.move_name = 'Bolt Strike') THEN TRUE
        WHEN (pm.pokemon_name = 'Reshiram' AND pm.move_name = 'Blue Flare') THEN TRUE
        
        -- Other notable signature moves
        WHEN (pm.pokemon_name = 'Snorlax' AND pm.move_name = 'Pulverizing Pancake') THEN TRUE
        WHEN (pm.pokemon_name = 'Marshadow' AND pm.move_name = 'Spectral Thief') THEN TRUE
        WHEN (pm.pokemon_name = 'Kommo-o' AND pm.move_name = 'Clangorous Soulblaze') THEN TRUE
        WHEN (pm.pokemon_name = 'Necrozma' AND pm.move_name = 'Photon Geyser') THEN TRUE
        WHEN (pm.pokemon_name = 'Solgaleo' AND pm.move_name = 'Sunsteel Strike') THEN TRUE
        WHEN (pm.pokemon_name = 'Lunala' AND pm.move_name = 'Moongeist Beam') THEN TRUE
        WHEN (pm.pokemon_name = 'Zeraora' AND pm.move_name = 'Plasma Fists') THEN TRUE
        WHEN (pm.pokemon_name = 'Mew' AND pm.move_name = 'Genesis Supernova') THEN TRUE
        
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
    END AS learn_method_type,
    
    -- Move priority classification (improved logic)
    CASE
        WHEN is_signature_move THEN 'Signature'
        WHEN pm.has_stab AND pm.level_learned_at <= 20 AND COALESCE(m.power, 0) >= 80 THEN 'High'
        WHEN pm.has_stab AND COALESCE(m.power, 0) >= 90 THEN 'High'
        WHEN pm.has_stab AND pm.level_learned_at <= 50 THEN 'Medium-High'
        WHEN pm.has_stab THEN 'Medium'
        WHEN pm.learn_method = 'machine' AND COALESCE(m.power, 0) >= 90 THEN 'TM-High'
        WHEN pm.learn_method = 'machine' THEN 'TM-Standard'
        WHEN pm.learn_method = 'tutor' AND COALESCE(m.power, 0) >= 80 THEN 'Tutor-High'
        WHEN COALESCE(m.power, 0) >= 100 THEN 'High-Power'
        ELSE 'Low'
    END AS move_priority,
    
    -- Competitive relevance (new)
    CASE
        -- Signature and high-power STAB moves
        WHEN is_signature_move THEN 5
        -- Strong STAB moves with good accuracy
        WHEN pm.has_stab AND COALESCE(m.power, 0) >= 90 AND COALESCE(m.accuracy, 0) >= 90 THEN 5
        -- Coverage moves (strong non-STAB)
        WHEN NOT pm.has_stab AND COALESCE(m.power, 0) >= 90 AND COALESCE(m.accuracy, 0) >= 90 THEN 4
        -- Medium-strong STAB moves
        WHEN pm.has_stab AND COALESCE(m.power, 0) >= 70 THEN 4
        -- Key support moves
        WHEN pm.move_name IN ('Stealth Rock', 'Toxic', 'Will-O-Wisp', 'Thunder Wave', 'Spikes', 
                            'Recover', 'Wish', 'Defog', 'Rapid Spin', 'Protect', 'Substitute', 
                            'Swords Dance', 'Nasty Plot', 'Dragon Dance', 'Calm Mind', 'Agility',
                            'Quiver Dance', 'Tailwind', 'Reflect', 'Light Screen') THEN 5
        -- Status moves are generally useful
        WHEN COALESCE(m.damage_class, 'Unknown') = 'status' THEN 3
        -- Weak moves
        WHEN COALESCE(m.power, 0) <= 40 THEN 1
        -- Everything else
        ELSE 2
    END AS competitive_relevance,
    
    -- Move generation match (new - indicates if move is from same generation as Pokemon)
    CASE
        WHEN pm.generation_number = COALESCE(m.generation_id, 0) THEN TRUE
        ELSE FALSE
    END AS is_same_generation,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM pokemon_moves pm
LEFT JOIN {{ ref('dim_pokemon_moves') }} m ON pm.move_name = m.name
ORDER BY pm.pokemon_id, competitive_relevance DESC, pm.level_learned_at