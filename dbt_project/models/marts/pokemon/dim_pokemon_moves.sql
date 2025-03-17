{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['move_id']}, {'columns': ['name']}, {'columns': ['type']}],
    unique_key = 'move_key'
  )
}}

/*
  Model: dim_pokemon_moves
  Description: Dimension table for Pokémon moves with comprehensive attributes
  
  Notes:
  - Provides detailed information about all moves in the Pokémon games
  - Contains move statistics like power, accuracy, and PP
  - Classifies moves by type, category, and effect
  - Includes generation information and competitive relevance
  - Serves as a reference dimension for move-related analyses
*/

WITH base_moves AS (
    SELECT
        id AS move_id,
        name,
        NULLIF(type, 'unknown') AS type,
        NULLIF(power::TEXT, 'null')::INTEGER AS power,
        NULLIF(pp::TEXT, 'null')::INTEGER AS pp,
        NULLIF(accuracy::TEXT, 'null')::INTEGER AS accuracy,
        priority,
        damage_class,
        effect_text,
        effect_chance,
        generation_id
    FROM {{ ref('stg_pokeapi_moves') }}
    WHERE id IS NOT NULL
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['move_id']) }} AS move_key,
    
    -- Core identifiers
    move_id,
    name,
    
    -- Move attributes
    type,
    COALESCE(power, 0) AS power,
    COALESCE(pp, 0) AS pp,
    COALESCE(accuracy, 0) AS accuracy,
    COALESCE(priority, 0) AS priority,
    COALESCE(damage_class, 'physical') AS damage_class,
    
    -- Effect information
    effect_text,
    COALESCE(effect_chance, 0) AS effect_chance,
    
    -- Generation information
    COALESCE(generation_id, 1) AS generation_id,
    
    -- Move category classification
    CASE
        WHEN damage_class = 'status' THEN 'Status'
        WHEN power IS NULL OR power = 0 THEN 'Status'
        WHEN power < 40 THEN 'Weak'
        WHEN power < 70 THEN 'Medium'
        WHEN power < 90 THEN 'Strong'
        WHEN power < 110 THEN 'Very Strong'
        ELSE 'Extreme'
    END AS power_category,
    
    -- Accuracy classification
    CASE
        WHEN accuracy IS NULL THEN 'Always Hits'
        WHEN accuracy < 50 THEN 'Very Inaccurate'
        WHEN accuracy < 70 THEN 'Inaccurate'
        WHEN accuracy < 85 THEN 'Moderate'
        WHEN accuracy < 95 THEN 'Accurate'
        WHEN accuracy < 100 THEN 'Very Accurate'
        WHEN accuracy = 100 THEN 'Perfect'
        ELSE 'Unknown'
    END AS accuracy_category,
    
    -- Move effect category based on effect text
    CASE
        WHEN effect_text LIKE '%boost%' OR 
             effect_text LIKE '%raise%' OR 
             effect_text LIKE '%increase%' THEN 'Stat Boost'
        WHEN effect_text LIKE '%lower%' OR 
             effect_text LIKE '%decrease%' OR 
             effect_text LIKE '%reduce%' THEN 'Stat Reduction'
        WHEN effect_text LIKE '%paralyze%' THEN 'Paralyze'
        WHEN effect_text LIKE '%burn%' THEN 'Burn'
        WHEN effect_text LIKE '%poison%' OR 
             effect_text LIKE '%toxic%' THEN 'Poison'
        WHEN effect_text LIKE '%sleep%' THEN 'Sleep'
        WHEN effect_text LIKE '%freeze%' THEN 'Freeze'
        WHEN effect_text LIKE '%confus%' THEN 'Confusion'
        WHEN effect_text LIKE '%flinch%' THEN 'Flinch'
        WHEN effect_text LIKE '%trap%' THEN 'Trap'
        WHEN effect_text LIKE '%heal%' OR 
             effect_text LIKE '%restore%' THEN 'Healing'
        WHEN effect_text LIKE '%protect%' OR 
             effect_text LIKE '%protect%' THEN 'Protection'
        WHEN effect_text LIKE '%critical%' THEN 'Critical Hit'
        WHEN effect_text LIKE '%priority%' OR priority > 0 THEN 'Priority'
        WHEN damage_class = 'status' THEN 'Status Effect'
        WHEN power > 0 THEN 'Direct Damage'
        ELSE 'Other'
    END AS effect_category,
    
    -- Competitive relevance score (1-10)
    CASE
        -- Extremely useful moves in competitive
        WHEN name IN ('Stealth Rock', 'Spikes', 'Toxic Spikes', 'Defog', 'Rapid Spin',
                   'Recover', 'Wish', 'Protect', 'Substitute', 'Will-O-Wisp',
                   'Scald', 'Knock Off', 'U-turn', 'Volt Switch', 'Toxic',
                   'Thunder Wave', 'Dragon Dance', 'Swords Dance', 'Nasty Plot',
                   'Calm Mind', 'Quiver Dance', 'Roost', 'Leech Seed') THEN 10
                   
        -- Very strong attacks and utility moves
        WHEN (power > 100 AND accuracy >= 90) OR 
             name IN ('Close Combat', 'Earthquake', 'Ice Beam', 'Thunderbolt',
                   'Flamethrower', 'Surf', 'Stone Edge', 'Focus Blast',
                   'Shadow Ball', 'Psyshock', 'Earth Power', 'Draco Meteor', 
                   'Moonblast', 'Play Rough', 'Gunk Shot', 'Brave Bird') THEN 9
                   
        -- Strong utility and common attacks
        WHEN (power >= 80 AND accuracy >= 85) OR
             name IN ('Taunt', 'Encore', 'Trick', 'Toxic', 'Synthesis', 
                   'Aromatherapy', 'Heal Bell', 'Sticky Web', 'Trick Room',
                   'Tailwind', 'Moonlight', 'Morning Sun') THEN 8
                   
        -- Useful moves but not top tier
        WHEN (power >= 70 AND accuracy >= 80) OR
             name IN ('Light Screen', 'Reflect', 'Hypnosis', 'Sleep Powder',
                   'Stun Spore', 'Thunder Wave') THEN 7
                   
        -- Standard damage moves with decent stats
        WHEN power >= 60 AND accuracy >= 90 THEN 6
        
        -- Status moves without specific utility
        WHEN damage_class = 'status' THEN 5
        
        -- Weak but accurate moves
        WHEN power < 60 AND accuracy > 90 THEN 4
        
        -- Inaccurate moves
        WHEN accuracy < 80 AND power > 0 THEN 3
        
        -- Very weak moves
        WHEN power < 40 THEN 2
        
        -- Other moves
        ELSE 1
    END AS competitive_score,
    
    -- Move uniqueness rating
    CASE
        -- Signature moves
        WHEN name IN ('Spacial Rend', 'Roar of Time', 'Seed Flare', 'Blue Flare', 'Bolt Strike',
                   'Fusion Flare', 'Fusion Bolt', 'Origin Pulse', 'Precipice Blades',
                   'Dragon Ascent', 'Sacred Fire', 'Aeroblast', 'Shadow Force',
                   'Doom Desire', 'Psycho Boost', 'Lunar Dance', 'Magma Storm',
                   'Crush Grip', 'Judgment', 'Secret Sword', 'Relic Song', 'Light of Ruin',
                   'Steam Eruption', 'Core Enforcer', 'Sunsteel Strike', 'Moongeist Beam',
                   'Photon Geyser', 'Spectral Thief', 'Plasma Fists') THEN 'Signature'
                   
        -- Very rare moves (limited distribution)
        WHEN name IN ('Shell Smash', 'Quiver Dance', 'Tail Glow', 'Dragon Dance',
                   'Shift Gear', 'Coil', 'Geomancy', 'Mind Blown', 'Oblivion Wing',
                   'Thousand Arrows', 'Thousand Waves', 'Diamond Storm') THEN 'Very Rare'
                   
        -- Rare but distributed moves
        WHEN name IN ('Spore', 'Dark Void', 'Healing Wish', 'Lunar Dance', 
                   'Shore Up', 'Belly Drum', 'Fiery Dance', 'King\'s Shield',
                   'Spiky Shield', 'Baneful Bunker', 'Parting Shot') THEN 'Rare'
                   
        -- Uncommon moves
        WHEN name IN ('Leech Seed', 'Aromatherapy', 'Heal Bell', 'Sticky Web',
                   'Defog', 'Rapid Spin', 'Extreme Speed', 'Sucker Punch',
                   'Bullet Punch', 'Aqua Jet', 'Mach Punch', 'Ice Shard') THEN 'Uncommon'
                   
        -- Common coverage moves
        WHEN name IN ('Ice Beam', 'Thunderbolt', 'Flamethrower', 'Surf',
                   'Earthquake', 'Stone Edge', 'Close Combat', 'Shadow Ball') THEN 'Standard'
                   
        -- Very common moves
        WHEN name IN ('Toxic', 'Protect', 'Rest', 'Sleep Talk', 'Substitute') THEN 'Common'
                   
        -- Handle everything else based on damage class
        WHEN damage_class = 'status' THEN 'Status'
        ELSE 'Standard'
    END AS move_rarity,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM base_moves
ORDER BY type, power DESC NULLS LAST, name