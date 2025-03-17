{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['ability_id']}, {'columns': ['ability_name']}],
    unique_key = 'ability_id'
  )
}}

/*
  Model: dim_pokemon_abilities
  Description: Dimension table for Pokémon abilities
  
  Notes:
  - Analyzes ability distribution across all Pokémon
  - Categorizes abilities by effect type and competitive tier
  - Calculates rarity metrics and battle style classifications
  - Identifies hidden abilities and their gameplay significance
  - Provides comprehensive categorization for analysis
  - Fixed string literals to use proper PostgreSQL escaping
  - Using E-string syntax for strings containing apostrophes
  - Maintaining consistent naming format with PokeAPI
*/

WITH ability_usage AS (
    -- Count how many Pokemon have each ability with improved error handling
    SELECT 
        LOWER(COALESCE(ability_ref->'ability'->>'name', 'unknown')) AS ability_name,
        COUNT(*) AS num_pokemon
    FROM {{ ref('stg_pokeapi_pokemon') }} p
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE 
            WHEN p.ability_list IS NULL OR p.ability_list::text = 'null' THEN '[]'::jsonb
            ELSE p.ability_list
        END
    ) AS ability_ref
    WHERE ability_ref->'ability'->>'name' IS NOT NULL
    GROUP BY ability_ref->'ability'->>'name'
),

-- Calculate ability rarity percentiles
ability_ranks AS (
    SELECT
        ability_name,
        num_pokemon,
        PERCENT_RANK() OVER (ORDER BY num_pokemon) AS rarity_percentile
    FROM ability_usage
),

-- Classify abilities by effect type with greatly expanded categories
ability_attributes AS (
    SELECT
        ability_name,
        CASE
            -- Stat modifiers (expanded)
            WHEN ability_name IN ('Intimidate', 'Moxie', 'Guts', 'Huge Power', 'Pure Power', 'Beast Boost',
                                'Defiant', 'Contrary', 'Simple', 'Competitive', 'Anger Point', 'Weak Armor',
                                'Speed Boost', 'Moody', 'Hustle', 'Sheer Force', 'No Guard', 'Technician',
                                'Tinted Lens', 'Unburden', 'Victory Star') THEN 'Stat Modifier'
                                
            -- Immunities (expanded)
            WHEN ability_name IN ('Levitate', 'Immunity', 'Water Absorb', 'Volt Absorb', 'Flash Fire',
                                'Motor Drive', 'Lightning Rod', 'Storm Drain', 'Sap Sipper', 'Dry Skin',
                                'Earth Eater', 'Wonder Guard', 'Soundproof', 'Water Bubble', 'Fluffy',
                                'Disguise', 'Thick Fat', 'Heatproof', 'Justified', 'Bulletproof', 
                                'Filter', 'Solid Rock', 'Prism Armor', 'Magic Bounce') THEN 'Immunity'
                                
            -- Speed control (expanded)
            WHEN ability_name IN ('Speed Boost', 'Chlorophyll', 'Swift Swim', 'Sand Rush', 'Slush Rush',
                                'Surge Surfer', 'Quick Feet', 'Unburden', 'Steadfast', 'Slow Start',
                                'Quick Draw', 'Gale Wings') THEN 'Speed'
                                
            -- Weather abilities
            WHEN ability_name IN ('Drought', 'Drizzle', 'Sand Stream', 'Snow Warning',
                                'Desolate Land', 'Primordial Sea', 'Delta Stream', 
                                'Sand Spit', 'Ice Face') THEN 'Weather'
                                
            -- Damage boosters (expanded)
            WHEN ability_name IN ('Blaze', 'Torrent', 'Overgrow', 'Swarm', 'Iron Fist',
                                'Adaptability', 'Aerilate', 'Pixilate', 'Refrigerate', 'Galvanize',
                                'Solar Power', 'Steelworker', 'Reckless', 'Mega Launcher',
                                'Strong Jaw', 'Tough Claws', 'Technician', 'Analytic',
                                'Punk Rock', E'Dragon\'s Maw', 'Transistor') THEN 'Damage Boost'
                                
            -- Opponent effects (expanded)
            WHEN ability_name IN ('Pressure', 'Unnerve', 'Intimidate', 'Mummy', 'Gooey', 'Tangling Hair',
                                'Shadow Tag', 'Arena Trap', 'Magnet Pull', 'Stall', 'Cute Charm',
                                'Rivalry', 'Slow Start', 'Truant', 'Cotton Down', 'Neutralizing Gas',
                                'Perish Body', 'Gorilla Tactics', 'Intrepid Sword') THEN 'Opponent Effect'
                                
            -- Contact effects (expanded)
            WHEN ability_name IN ('Synchronize', 'Effect Spore', 'Static', 'Flame Body', 'Poison Point',
                                'Rough Skin', 'Iron Barbs', 'Pickpocket', 'Gooey', 'Tangling Hair',
                                'Wandering Spirit', 'Cotton Down', 'Mummy', 'Cursed Body',
                                'Perish Body') THEN 'Contact Effect'
                                
            -- Healing abilities
            WHEN ability_name IN ('Regenerator', 'Natural Cure', 'Shed Skin', 'Poison Heal', 
                                'Hydration', 'Ice Body', 'Rain Dish', 'Healer', 'Water Bubble', 
                                'Ice Face', 'Disguise') THEN 'Healing'
                                
            -- Entry hazard abilities
            WHEN ability_name IN ('Spikes', 'Stealth Rock', 'Sticky Web', 'Toxic Spikes') THEN 'Entry Hazard'
            
            -- Form-changing
            WHEN ability_name IN ('Protean', 'Libero', 'Color Change', 'Forecast', 'Mimicry',
                                'Stance Change', 'Battle Bond', 'Power Construct', 
                                'Shields Down', 'Schooling') THEN 'Form Change'
                                
            -- Defensive abilities
            WHEN ability_name IN ('Multiscale', 'Shadow Shield', 'Sturdy', 'Battle Armor', 'Shell Armor',
                                'Filter', 'Solid Rock', 'Prism Armor', 'Fluffy', 'Thick Fat',
                                'Magic Guard', 'Disguise', 'Ice Face', 'Marvel Scale',
                                'Friend Guard', 'Dauntless Shield') THEN 'Defensive'
                                
            -- Item-related
            WHEN ability_name IN ('Pickup', 'Honey Gather', 'Frisk', 'Magician', 'Pickpocket',
                                'Sticky Hold', 'Unburden', 'Klutz', 'Harvest', 'Cheek Pouch',
                                'Ripen', 'Ball Fetch') THEN 'Item Effect'
                                
            -- Status condition-related
            WHEN ability_name IN ('Immunity', 'Limber', 'Insomnia', 'Vital Spirit', 'Water Veil',
                                'Magma Armor', 'Oblivious', 'Own Tempo', 'Inner Focus',
                                'Shield Dust', 'Leaf Guard', 'Pastel Veil', 'Good as Gold') THEN 'Status Prevention'
            
            -- Priority move effects
            WHEN ability_name IN ('Prankster', 'Gale Wings', 'Triage', 'Queenly Majesty', 
                                'Dazzling', 'Armor Tail') THEN 'Priority'
                                
            ELSE 'Other'
        END AS effect_type,
        
        -- Expanded competitive tier rankings
        CASE
            -- S-Tier: Game-changing abilities
            WHEN ability_name IN ('Wonder Guard', 'Huge Power', 'Pure Power', 'Speed Boost', 
                               'Drought', 'Drizzle', 'Sand Stream', 'Snow Warning',
                               'Shadow Tag', 'Intimidate', 'Beast Boost', 'Protean', 'Magic Guard',
                               'Prankster', 'Unaware', 'Imposter', 'Desolate Land', 'Primordial Sea',
                               'Delta Stream', 'Soul-Heart', 'Libero', 'Intrepid Sword',
                               'Gorilla Tactics', 'As One') THEN 'S'
                               
            -- A-Tier: Very strong abilities
            WHEN ability_name IN ('Moxie', 'Adaptability', 'Serene Grace', 'Guts', 'Levitate', 
                               'Multiscale', 'Regenerator', 'Unaware', 'Water Absorb',
                               'Volt Absorb', 'Disguise', 'Slush Rush', 'Swift Swim', 'Sand Rush',
                               'Chlorophyll', 'Queenly Majesty', 'Dazzling', 'Sturdy', 'Electric Surge',
                               'Psychic Surge', 'Grassy Surge', 'Misty Surge', 'Power Construct',
                               'Shields Down', 'Contrary', 'Mold Breaker', 'Parental Bond') THEN 'A'
                               
            -- B-Tier: Good abilities
            WHEN ability_name IN ('Flash Fire', 'Poison Heal', 'Thick Fat', 'Technician', 
                               'Sheer Force', 'Tinted Lens', 'Fairy Aura', 'Dark Aura',
                               'Aura Break', 'No Guard', 'Competitive', 'Simple', 'Unburden',
                               'Mummy', 'Iron Barbs', 'Rough Skin', 'Solar Power', 'Analytic',
                               'Steelworker', 'Aerilate', 'Pixilate', 'Refrigerate', 'Neuroforce',
                               'Galvanize', 'Trace') THEN 'B'
                               
            -- C-Tier: Average abilities
            WHEN ability_name IN ('Blaze', 'Torrent', 'Overgrow', 'Swarm', 'Natural Cure', 
                               'Clear Body', 'Sticky Hold', 'Shed Skin', 'Mold Breaker',
                               'Pressure', 'Synchronize', 'Ice Body', 'Rain Dish', 'Hustle',
                               'Filter', 'Solid Rock', 'Infiltrator', 'Super Luck', 'Harvest',
                               'Big Pecks', 'Early Bird', 'Frisk', 'Rock Head', 'Steadfast',
                               'Sniper', 'Heavy Metal', 'Light Metal', 'Hydration') THEN 'C'
                               
            -- D-Tier: Weak or very situational abilities
            WHEN ability_name IN ('Run Away', 'Honey Gather', 'Illuminate', 'Stench', 'Stall',
                               'Suction Cups', 'Shell Armor', 'Battle Armor', 'Ball Fetch',
                               'Pickup', 'Klutz', 'Slow Start', 'Truant', 'Defeatist',
                               'Forecast', 'Healer', 'Friend Guard', 'Minus', 'Plus', 'Anticipation',
                               'Flower Gift', 'Forewarn', 'Rivalry', 'Victory Star',
                               'Leaf Guard', 'Tangled Feet') THEN 'D'
                               
            -- F-Tier: Detrimental or extremely niche abilities
            WHEN ability_name IN ('Defeatist', 'Slow Start', 'Truant', 'Stall', 'Klutz',
                               'Illuminate', 'Minus', 'Plus', 'Flower Veil', 'Friend Guard') THEN 'F'
                               
            ELSE 'Unclassified'
        END AS tier
    FROM ability_ranks
)

SELECT
    -- Primary key (using surrogate key pattern)
    {{ dbt_utils.generate_surrogate_key(['u.ability_name']) }} AS ability_key,
    ROW_NUMBER() OVER (ORDER BY u.ability_name) AS ability_id,
    
    -- Core attributes
    u.ability_name,
    u.num_pokemon,
    a.effect_type,
    a.tier,
    
    -- Calculate rarity - improved with percentiles
    CASE
        WHEN r.rarity_percentile >= 0.95 THEN 'Ultra Rare'
        WHEN r.rarity_percentile >= 0.80 THEN 'Very Rare'
        WHEN r.rarity_percentile >= 0.60 THEN 'Rare'
        WHEN r.rarity_percentile >= 0.40 THEN 'Uncommon'
        WHEN r.rarity_percentile >= 0.20 THEN 'Common'
        ELSE 'Very Common'
    END AS rarity,
    
    -- Enhanced hidden ability identification with more comprehensive list
    CASE
        WHEN u.ability_name IN (
            -- Gen 5 Dream World abilities
            'Analytic', 'Chlorophyll', 'Cloud Nine', 'Damp', 'Defiant', 'Drought', 'Drizzle',
            'Intimidate', 'Lightningrod', 'Moody', 'Moxie', 'Multiscale', 'Overcoat', 'Poison Touch',
            'Prankster', 'Rain Dish', 'Regenerator', 'Sand Force', 'Sand Rush', 'Sand Veil',
            'Serene Grace', 'Sheer Force', 'Sniper', 'Snow Warning', 'Speed Boost', 'Technician',
            'Telepathy', 'Unaware', 'Unnerve',
            
            -- Gen 6 notable hidden abilities
            'Gale Wings', 'Protean', 'Magic Guard', 'Magic Bounce', 'Harvest', 'Infiltrator',
            'Tinted Lens', 'Gooey', 'Aroma Veil', 'Bulletproof', 'Magician', 'Pickpocket',
            
            -- Gen 7 notable hidden abilities
            'Power of Alchemy', 'Beast Boost', 'RKS System', 'Slush Rush', 'Surge Surfer',
            'Water Compaction', 'Queenly Majesty', 'Stamina', 'Water Bubble', 'Steelworker',
            
            -- Gen 8 notable hidden abilities with fixed string literals
            'Libero', 'Quick Draw', 'Ice Scales', 'Punk Rock', 'Mirror Armor', 'Neutralizing Gas',
            'Power Spot', 'Ripen', 'Steam Engine', 'Sand Spit', 'Cotton Down', 'Gorilla Tactics',
            
            -- Other known hidden abilities that are particularly strong
            'Adaptability', 'Competitive', 'Contrary', 'Cursed Body', 'Guts', 'Hustle', 'Marvel Scale',
            'No Guard', 'Poison Heal', 'Solar Power', 'Swift Swim', 'Thick Fat', 'Unburden',
            'Water Absorb', 'Wonder Skin'
        ) THEN TRUE
        ELSE FALSE
    END AS likely_hidden,
    
    -- Battle style classification - fixed string literals
    CASE
        -- Offensive battle style
        WHEN a.effect_type IN ('Damage Boost', 'Speed') OR 
             u.ability_name IN ('Moxie', 'Adaptability', 'Beast Boost', 'Huge Power', 'Pure Power',
                             'Sheer Force', 'Tough Claws', 'Strong Jaw', 'Technician', 'Gorilla Tactics',
                             'Intrepid Sword', 'Contrary', 'Aerilate', 'Pixilate', 'Refrigerate',
                             'Galvanize', 'Steelworker', 'Protean', 'Libero', E'dragon''s-maw', 'Transistor',
                             'Guts', 'No Guard', 'Solar Power', 'Tinted Lens', 'Mega Launcher', 'Sniper')
        THEN 'Offensive'
        
        -- Defensive battle style
        WHEN a.effect_type IN ('Immunity', 'Defensive', 'Status Prevention', 'Healing') OR
             u.ability_name IN ('Multiscale', 'Shadow Shield', 'Fur Coat', 'Fluffy', 'Ice Face',
                             'Disguise', 'Intimidate', 'Filter', 'Solid Rock', 'Prism Armor',
                             'Sturdy', 'Magic Guard', 'Thick Fat', 'Wonder Guard', 'Levitate',
                             'Water Absorb', 'Volt Absorb', 'Flash Fire', 'Bulletproof', 'Heatproof',
                             'Battle Armor', 'Shell Armor', 'Dauntless Shield', 'Ice Scales')
        THEN 'Defensive'
        
        -- Utility/Support battle style
        WHEN a.effect_type IN ('Weather', 'Opponent Effect', 'Entry Hazard', 'Priority', 'Contact Effect') OR
             u.ability_name IN ('Drought', 'Drizzle', 'Sand Stream', 'Snow Warning', 'Unaware',
                             'Prankster', 'Queenly Majesty', 'Dazzling', 'Pressure', 'Trace',
                             'Synchronize', 'Neutralizing Gas', 'Harvest', 'Healer', 'Regenerator',
                             'Unnerve', 'Sticky Hold', 'Magnet Pull', 'Arena Trap', 'Shadow Tag')
        THEN 'Utility'
        
        -- Setup-based battle style
        WHEN u.ability_name IN ('Speed Boost', 'Moody', 'Simple', 'Contrary', 'Power Construct',
                             'Shields Down', 'Battle Bond', 'Schooling', 'Stance Change',
                             'Berserk', 'Weak Armor', 'Slush Rush', 'Swift Swim', 'Chlorophyll',
                             'Sand Rush', 'Surge Surfer', 'Unburden', 'Compound Eyes')
        THEN 'Setup'
        
        -- Default
        ELSE 'Miscellaneous'
    END AS battle_style,
    
    -- Improved generation introduced information
    CASE
        WHEN u.ability_name IN ('Overgrow', 'Blaze', 'Torrent', 'Swarm', 'Intimidate', 'Static', 'Levitate',
                             'Sturdy', 'Chlorophyll', 'Wonder Guard', 'Speed Boost', 'Synchronize', 'Keen Eye',
                             'Hyper Cutter', 'Guts', 'Sand Stream', 'Drizzle', 'Drought', 'Flash Fire',
                             'Wonder Guard', 'Pressure', 'Thick Fat', 'Hustle', 'Truant', 'Cloud Nine',
                             'Compound Eyes', 'Battle Armor', 'Clear Body', 'Swift Swim', 'Huge Power',
                             'Sand Veil', 'Arena Trap', 'Water Veil', 'Liquid Ooze', 'Rock Head', 'Early Bird',
                             'Sticky Hold', 'Shed Skin', 'Run Away', 'Serene Grace', 'Shadow Tag', 'Pure Power',
                             'Vital Spirit', 'White Smoke', 'Shell Armor', 'Air Lock') THEN 3  -- Gen 3 (first with abilities)
                             
        WHEN u.ability_name IN ('Aftermath', 'Anticipation', 'Bad Dreams', 'Download', 'Dry Skin',
                             'Filter', 'Flower Gift', 'Forewarn', 'Frisk', 'Gluttony', 'Heatproof',
                             'Honey Gather', 'Hydration', 'Ice Body', 'Iron Fist', 'Klutz', 'Leaf Guard',
                             'Magic Guard', 'Mold Breaker', 'Motor Drive', 'Multitype', 'No Guard',
                             'Normalize', 'Poison Heal', 'Quick Feet', 'Reckless', 'Rivalry', 'Scrappy',
                             'Simple', 'Skill Link', 'Slow Start', 'Sniper', 'Snow Cloak', 'Snow Warning',
                             'Solar Power', 'Solid Rock', 'Stall', 'Steadfast', 'Storm Drain', 'Suction Cups',
                             'Tangled Feet', 'Technician', 'Tinted Lens', 'Unaware') THEN 4  -- Gen 4
                             
        WHEN u.ability_name IN ('Analytic', 'Big Pecks', 'Contrary', 'Cursed Body', 'Defeatist', 'Defiant',
                             'Flare Boost', 'Friend Guard', 'Harvest', 'Healer', 'Heavy Metal', 'Illusion',
                             'Imposter', 'Infiltrator', 'Iron Barbs', 'Light Metal', 'Magic Bounce', 'Moody',
                             'Moxie', 'Multiscale', 'Mummy', 'Overcoat', 'Pickpocket', 'Poison Touch',
                             'Prankster', 'Rattled', 'Regenerator', 'Sand Force', 'Sand Rush', 'Sap Sipper',
                             'Sheer Force', 'Telepathy', 'Teravolt', 'Toxic Boost', 'Turboblaze', 'Unnerve',
                             'Victory Star', 'Zen Mode') THEN 5  -- Gen 5
                             
        WHEN u.ability_name IN ('Aroma Veil', 'Aura Break', 'Bulletproof', 'Cheek Pouch', 'Competitive',
                             'Dark Aura', 'Fairy Aura', 'Flower Veil', 'Fur Coat', 'Gale Wings',
                             'Gooey', 'Grass Pelt', 'Magician', 'Mega Launcher', 'Parental Bond',
                             'Pixilate', 'Protean', 'Refrigerate', 'Strong Jaw', 'Stance Change',
                             'Sweet Veil', 'Symbiosis', 'Tough Claws') THEN 6  -- Gen 6
                             
        WHEN u.ability_name IN ('Battery', 'Beast Boost', 'Comatose', 'Corrosion', 'Dazzling',
                             'Disguise', 'Electric Surge', 'Emergency Exit', 'Fluffy', 'Full Metal Body',
                             'Galvanize', 'Grassy Surge', 'Innards Out', 'Liquid Voice', 'Long Reach',
                             'Merciless', 'Misty Surge', 'Neuroforce', 'Power Construct', 'Power of Alchemy',
                             'Prism Armor', 'Psychic Surge', 'Queenly Majesty', 'RKS System', 'Receiver',
                             'Schooling', 'Shadow Shield', 'Shields Down', 'Slush Rush', 'Soul-Heart',
                             'Stamina', 'Stakeout', 'Steelworker', 'Surge Surfer', 'Tangling Hair',
                             'Triage', 'Water Bubble', 'Water Compaction', 'Wimp Out') THEN 7  -- Gen 7
                             
        WHEN u.ability_name IN ('As One', 'Ball Fetch', 'Cotton Down', 'Curious Medicine', 'Dauntless Shield',
                             E'dragon''s-maw', 'Gorilla Tactics', 'Gulp Missile', 'Hunger Switch', 'Ice Face',
                             'Ice Scales', 'Intrepid Sword', 'Libero', 'Mirror Armor', 'Neutralizing Gas',
                             'Pastel Veil', 'Perish Body', 'Power Spot', 'Propeller Tail', 'Punk Rock',
                             'Quick Draw', 'Ripen', 'Sand Spit', 'Screen Cleaner', 'Stalwart',
                             'Steam Engine', 'Steely Spirit', 'Transistor', 'Unseen Fist', 'Wandering Spirit') 
        THEN 8  -- Gen 8
                             
        WHEN u.ability_name IN ('Angular Wing', 'Armor Tail', 'Beads of Ruin', 'Commander', 'Cud Chew',
                             'Earth Eater', 'Electromorphosis', 'Good as Gold', 'Guard Dog', 'Hadron Engine',
                             'Lingering Aroma', 'Mycelium Might', 'Opportunist', 'Orichalcum Pulse', 'Protosynthesis',
                             'Purifying Salt', 'Quark Drive', 'Seed Sower', 'Sharpness', 'Supreme Overlord',
                             'Sword of Ruin', 'Tablets of Ruin', 'Thermal Exchange', 'Toxic Debris', 'Vessel of Ruin',
                             'Well-Baked Body', 'Wind Power', 'Wind Rider', 'Zero to Hero') THEN 9  -- Gen 9
                             
        ELSE NULL  -- Likely errors or future abilities
    END AS generation_introduced,
    
    -- Numerical power rating (1-10 scale)
    CASE
        WHEN a.tier = 'S' THEN 10
        WHEN a.tier = 'A' THEN 8
        WHEN a.tier = 'B' THEN 6
        WHEN a.tier = 'C' THEN 4
        WHEN a.tier = 'D' THEN 2
        WHEN a.tier = 'F' THEN 1
        ELSE 3
    END AS power_rating,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM ability_usage u
JOIN ability_ranks r ON u.ability_name = r.ability_name
LEFT JOIN ability_attributes a ON u.ability_name = a.ability_name
ORDER BY a.tier, u.num_pokemon DESC