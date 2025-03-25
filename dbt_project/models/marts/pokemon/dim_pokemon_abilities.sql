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
        COALESCE(NULLIF(p.abilities::text, 'null')::jsonb, '[]'::jsonb)
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
                             'Storm Drain', 'Water Bubble', 'Ice Body', 'Marvel Scale', 'Regenerator')
        THEN 'Defensive'
        
        -- Utility battle style
        WHEN a.effect_type IN ('Weather', 'Entry Hazard', 'Form Change', 'Item Effect', 'Priority') OR
             u.ability_name IN ('Drought', 'Drizzle', 'Sand Stream', 'Snow Warning', 'Prankster',
                             'Serene Grace', 'Queenly Majesty', 'Dazzling', 'Magic Bounce', 'Trace',
                             'Natural Cure', 'Shed Skin', 'Grassy Surge', 'Electric Surge', 'Psychic Surge',
                             'Misty Surge', 'Harvest', 'Pickpocket', 'Magician', 'Unburden', 'Hydration',
                             'Lightning Rod', 'Magnet Pull', 'Sticky Hold', 'Stealth Rock')
        THEN 'Utility'
        
        -- Support battle style
        WHEN a.effect_type IN ('Opponent Effect', 'Contact Effect') OR
             u.ability_name IN ('Intimidate', 'Unnerve', 'Pressure', 'Mummy', 'Synchronize',
                             'Effect Spore', 'Static', 'Flame Body', 'Poison Point', 'Gooey',
                             'Friend Guard', 'Healer', 'Cursed Body', 'Iron Barbs', 'Rough Skin',
                             'Aftermath', 'Frisk', 'Ice Face', 'Cotton Down', 'Neutralizing Gas',
                             'Arena Trap', 'Shadow Tag', 'Speed Boost', 'Immunity', 'Rain Dish')
        THEN 'Support'
        
        ELSE 'Balanced'
    END AS battle_style,
    
    -- Generation affinity using comprehensive list
    CASE
        -- Gen 1 signature/themed abilities
        WHEN u.ability_name IN ('Blaze', 'Torrent', 'Overgrow', 'Swarm', 'Static', 'Thick Fat',
                             'Shell Armor', 'Battle Armor', 'Early Bird', 'Chlorophyll',
                             'Rock Head', 'Sturdy', 'Guts', 'Run Away', 'Intimidate', 'Clear Body')
        THEN 1
        
        -- Gen 2 signature/themed abilities
        WHEN u.ability_name IN ('Flash Fire', 'Swift Swim', 'Inner Focus', 'Levitate',
                             'Forecast', 'Intimidate', 'Shed Skin', 'Rough Skin', 'Pressure',
                             'Trace', 'Pure Power', 'Huge Power', 'Shadow Tag', 'Wonder Guard',
                             'Synchronize', 'Natural Cure', 'Lightning Rod', 'Drizzle', 'Drought')
        THEN 2
        
        -- Gen 3 signature/themed abilities
        WHEN u.ability_name IN ('Sand Stream', 'Truant', 'Slaking', 'Soundproof', 'Magic Guard',
                             'Compoundeyes', 'Speed Boost', 'Marvel Scale', 'Steadfast',
                             'Poison Point', 'Air Lock', 'Filter', 'Solid Rock', 'Levitate')
        THEN 3
        
        -- Gen 4 signature/themed abilities
        WHEN u.ability_name IN ('Adaptability', 'Technician', 'Download', 'Motor Drive',
                             'Ice Body', 'Snow Warning', 'Snow Cloak', 'Slow Start', 'Bad Dreams',
                             'Multitype', 'Flower Gift', 'Iron Fist', 'Tinted Lens', 'Scrappy')
        THEN 4
        
        -- Gen 5 signature/themed abilities
        WHEN u.ability_name IN ('Teravolt', 'Turboblaze', 'Analytic', 'Sand Force', 'Sand Rush',
                             'Victory Star', 'Zen Mode', 'Defiant', 'Prankster', 'Illusion',
                             'Moxie', 'Justified', 'Unaware', 'Magic Bounce', 'Heavy Metal')
        THEN 5
        
        -- Gen 6 signature/themed abilities
        WHEN u.ability_name IN ('Protean', 'Aerilate', 'Pixilate', 'Refrigerate', 'Parental Bond',
                             'Dark Aura', 'Fairy Aura', 'Aura Break', 'Stance Change', 'Gale Wings',
                             'Sweet Veil', 'Gooey', 'Bulletproof', 'Competitive', 'Cheek Pouch')
        THEN 6
        
        -- Gen 7 signature/themed abilities
        WHEN u.ability_name IN ('Beast Boost', 'Disguise', 'RKS System', 'Electric Surge',
                             'Psychic Surge', 'Grassy Surge', 'Misty Surge', 'Full Metal Body',
                             'Neuroforce', 'Soul-Heart', 'Schooling', 'Shields Down', 'Fluffy')
        THEN 7
        
        -- Gen 8 signature/themed abilities
        WHEN u.ability_name IN ('Libero', 'Intrepid Sword', 'Dauntless Shield', 'Ball Fetch',
                             'Cotton Down', 'Steam Engine', 'Sand Spit', 'Mirror Armor',
                             'Hunger Switch', 'Ice Face', 'Power Spot', 'Ice Scales', 'Punk Rock',
                             'Gorilla Tactics', 'Neutralizing Gas', 'Ripen', 'Gulp Missile')
        THEN 8
        
        -- Default to most recent generation
        ELSE 8
    END AS generation_affinity,
    
    -- Additional stat context
    CASE
        WHEN a.effect_type = 'Stat Modifier' THEN
            CASE
                WHEN u.ability_name IN ('Huge Power', 'Pure Power', 'Gorilla Tactics',
                                    'Intrepid Sword', 'Moxie', 'Beast Boost', 'Swords Dance',
                                    'Dragon Dance') THEN 'Attack'
                                    
                WHEN u.ability_name IN ('Solar Power', 'Competitive', 'Beast Boost',
                                    'Soul-Heart', 'Nasty Plot', 'Calm Mind') THEN 'Special Attack'
                                    
                WHEN u.ability_name IN ('Speed Boost', 'Swift Swim', 'Chlorophyll',
                                    'Sand Rush', 'Slush Rush', 'Unburden', 'Surge Surfer') THEN 'Speed'
                                    
                WHEN u.ability_name IN ('Intimidate', 'Fur Coat', 'Marvel Scale',
                                    'Filter', 'Solid Rock', 'Prism Armor', 'Ice Face') THEN 'Defense'
                                    
                WHEN u.ability_name IN ('Multiscale', 'Shadow Shield', 'Natural Cure',
                                    'Regenerator', 'Hydration', 'Shed Skin') THEN 'HP/Recovery'
                                    
                ELSE 'Mixed'
            END
        ELSE NULL
    END AS primary_stat,
    
    -- Add data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM ability_usage u
JOIN ability_ranks r ON u.ability_name = r.ability_name
JOIN ability_attributes a ON u.ability_name = a.ability_name
ORDER BY u.num_pokemon DESC, u.ability_name