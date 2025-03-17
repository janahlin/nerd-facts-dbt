{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['pokemon_id']}, {'columns': ['ability_id']}],
    unique_key = 'pokemon_ability_id'
  )
}}

/*
  Model: bridge_pokemon_abilities
  Description: Bridge table connecting Pokémon to their abilities
  
  Notes:
  - Handles the many-to-many relationship between Pokémon and abilities
  - Extracts ability data from nested JSON in the pokémon staging model
  - Calculates synergy scores between Pokémon and their abilities
  - Provides context on whether abilities are hidden or standard
  - Links to both Pokémon and ability dimension tables with proper keys
*/

WITH pokemon_abilities AS (
    -- Extract ability references from the pokemon data with improved error handling
    SELECT
        p.id AS pokemon_id,
        p.name AS pokemon_name,
        p.primary_type,
        p.secondary_type,
        p.generation_number,
        p.is_legendary,
        p.is_mythical,
        p.total_base_stats,
        COALESCE(ability_ref->>'name', 'Unknown') AS ability_name,
        COALESCE((ability_ref->>'is_hidden')::boolean, FALSE) AS is_hidden,
        COALESCE((ability_ref->>'slot')::integer, 1) AS slot_number
    FROM {{ ref('stg_pokeapi_pokemon') }} p,
    LATERAL jsonb_array_elements(
        CASE WHEN p.abilities IS NULL OR p.abilities = 'null' THEN '[]'::jsonb
        ELSE p.abilities END
    ) AS ability_ref
    WHERE p.id IS NOT NULL
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['pa.pokemon_id', 'pa.ability_name']) }} AS pokemon_ability_id,
    
    -- Foreign keys with proper surrogate keys
    {{ dbt_utils.generate_surrogate_key(['pa.pokemon_id']) }} AS pokemon_key,
    {{ dbt_utils.generate_surrogate_key(['a.ability_id']) }} AS ability_key,
    
    -- Base identifiers
    pa.pokemon_id,
    pa.pokemon_name,
    pa.ability_name,
    pa.is_hidden,
    pa.slot_number,
    
    -- Reference data from ability dimension
    a.ability_id,
    COALESCE(a.effect_type, 'Unknown') AS effect_type,
    COALESCE(a.tier, 'Unknown') AS ability_tier,
    
    -- Synergy rating between Pokemon and ability with significantly expanded logic
    CASE
        -- Top competitive synergies (examples from competitive play)
        WHEN (pa.pokemon_name = 'Gengar' AND pa.ability_name = 'Levitate') THEN 5
        WHEN (pa.pokemon_name = 'Gyarados' AND pa.ability_name = 'Intimidate') THEN 5
        WHEN (pa.pokemon_name = 'Garchomp' AND pa.ability_name = 'Rough Skin') THEN 5
        WHEN (pa.pokemon_name = 'Dragonite' AND pa.ability_name = 'Multiscale') THEN 5
        WHEN (pa.pokemon_name = 'Tyranitar' AND pa.ability_name = 'Sand Stream') THEN 5
        WHEN (pa.pokemon_name = 'Ferrothorn' AND pa.ability_name = 'Iron Barbs') THEN 5
        WHEN (pa.pokemon_name = 'Excadrill' AND pa.ability_name = 'Sand Rush') THEN 5
        WHEN (pa.pokemon_name = 'Serperior' AND pa.ability_name = 'Contrary') THEN 5
        WHEN (pa.pokemon_name = 'Greninja' AND pa.ability_name = 'Protean') THEN 5
        WHEN (pa.pokemon_name = 'Blaziken' AND pa.ability_name = 'Speed Boost') THEN 5
        
        -- Weather abilities synergy with types
        WHEN (pa.ability_name = 'Drought' AND pa.primary_type = 'Fire') THEN 5
        WHEN (pa.ability_name = 'Drizzle' AND pa.primary_type = 'Water') THEN 5
        WHEN (pa.ability_name = 'Sand Stream' AND pa.primary_type IN ('Rock', 'Ground')) THEN 5
        WHEN (pa.ability_name = 'Snow Warning' AND pa.primary_type = 'Ice') THEN 5
        
        -- Type boosting abilities
        WHEN (pa.ability_name = 'Blaze' AND pa.primary_type = 'Fire') THEN 4
        WHEN (pa.ability_name = 'Torrent' AND pa.primary_type = 'Water') THEN 4
        WHEN (pa.ability_name = 'Overgrow' AND pa.primary_type = 'Grass') THEN 4
        WHEN (pa.ability_name = 'Swarm' AND pa.primary_type = 'Bug') THEN 4
        WHEN (pa.ability_name = 'Flash Fire' AND pa.primary_type = 'Fire') THEN 4
        WHEN (pa.ability_name = 'Water Absorb' AND pa.primary_type = 'Water') THEN 4
        WHEN (pa.ability_name = 'Motor Drive' AND pa.primary_type = 'Electric') THEN 4
        
        -- Type immunity abilities
        WHEN (pa.ability_name = 'Levitate' AND pa.primary_type NOT IN ('Flying', 'Ground')) THEN 4.5
        WHEN (pa.ability_name = 'Levitate' AND pa.primary_type IN ('Flying', 'Ground')) THEN 3.5 -- Redundant for Flying
        WHEN (pa.ability_name = 'Flash Fire' AND pa.primary_type NOT IN ('Fire')) THEN 4
        WHEN (pa.ability_name = 'Water Absorb' AND pa.primary_type NOT IN ('Water')) THEN 4
        WHEN (pa.ability_name = 'Lightning Rod' AND pa.primary_type NOT IN ('Electric')) THEN 4
        
        -- Physical vs Special synergy based on stats
        WHEN (pa.ability_name IN ('Intimidate', 'Marvel Scale', 'Guts') AND 
              pa.total_base_stats >= 500) THEN 4.5
        WHEN (pa.ability_name IN ('Huge Power', 'Pure Power', 'Sheer Force') AND 
              pa.total_base_stats >= 400) THEN 4.5
        
        -- High stat Pokémon with appropriate abilities
        WHEN (pa.ability_name IN ('Huge Power', 'Pure Power', 'Tough Claws', 'Technician') AND 
              pa.total_base_stats >= 500) THEN 4
        
        -- Legendary/Mythical synergies are often designed to be good
        WHEN (pa.is_legendary OR pa.is_mythical) AND pa.is_hidden = FALSE THEN 4.5
        
        -- Hidden abilities are often better (especially in later generations)
        WHEN pa.is_hidden AND pa.generation_number >= 5 THEN 4
        WHEN pa.is_hidden THEN 3.5
        
        -- Newer generations tend to have more balanced abilities
        WHEN pa.generation_number >= 6 AND pa.is_hidden = FALSE THEN 3.5
        
        -- Default synergy (still decent)
        ELSE 3
    END AS ability_synergy,
    
    -- Competitive relevance indicator
    CASE
        WHEN pa.ability_name IN (
            'Speed Boost', 'Protean', 'Intimidate', 'Drought', 'Drizzle', 
            'Sand Stream', 'Adaptability', 'Huge Power', 'Multiscale',
            'Magic Guard', 'Regenerator', 'Mold Breaker', 'Contrary',
            'Magic Bounce', 'Wonder Guard', 'Levitate', 'Unaware',
            'Disguise', 'Prankster', 'Queenly Majesty', 'Justified',
            'Serene Grace'
        ) THEN TRUE
        WHEN (pa.is_legendary OR pa.is_mythical) AND pa.is_hidden THEN TRUE
        ELSE FALSE
    END AS is_competitively_relevant,
    
    -- Generation relationship
    CASE 
        WHEN pa.ability_name IN ('Intimidate', 'Levitate', 'Chlorophyll', 
                              'Swift Swim', 'Sand Stream', 'Drought', 'Drizzle') 
             AND pa.generation_number <= 3 THEN 'Original Ability'
        WHEN pa.generation_number >= 6 AND pa.is_hidden THEN 'Modern Hidden Ability'
        WHEN pa.is_hidden THEN 'Hidden Ability'
        ELSE 'Standard Ability'
    END AS ability_classification,
    
    -- Add data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM pokemon_abilities pa
LEFT JOIN {{ ref('stg_pokeapi_pokemon') }} p ON pa.pokemon_id = p.id
LEFT JOIN {{ ref('dim_pokemon_abilities') }} a ON pa.ability_name = a.ability_name
ORDER BY pa.pokemon_id, pa.slot_number