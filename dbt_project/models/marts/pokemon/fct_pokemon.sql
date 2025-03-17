{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['pokemon_id']}, {'columns': ['primary_type', 'secondary_type']}],
    unique_key = 'pokemon_key'
  )
}}

/*
  Model: fct_pokemon
  Description: Core fact table for Pokémon data with comprehensive attributes and classifications
  
  Notes:
  - Contains essential information about each Pokémon species
  - Links to all related dimension tables (types, abilities, moves, etc.)
  - Provides both source fields and derived/calculated metrics
  - Includes generation, evolutionary, and classification data
  - Serves as the central entity for all Pokémon analysis
*/

WITH base_pokemon AS (
    SELECT
        id AS pokemon_id,
        name AS pokemon_name,
        primary_type,
        secondary_type,
        generation_number,
        is_legendary,
        is_mythical,
        height,
        weight,
        base_xp,
        abilities,
        moves,
        base_stat_hp,
        base_stat_attack,
        base_stat_defense,
        base_stat_special_attack,
        base_stat_special_defense,
        base_stat_speed,
        total_base_stats,
        capture_rate,
        base_happiness,
        evolution_chain_id,
        species_id
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE id IS NOT NULL
),

-- Count abilities and moves for each Pokémon
attribute_counts AS (
    SELECT
        pokemon_id,
        COALESCE(JSONB_ARRAY_LENGTH(abilities), 0) AS ability_count,
        COALESCE(JSONB_ARRAY_LENGTH(moves), 0) AS move_count
    FROM base_pokemon
),

-- Determine popularity tier based on known popular Pokémon
popularity AS (
    SELECT
        pokemon_id,
        CASE
            -- Gen 1 popular starters and mascots
            WHEN LOWER(pokemon_name) IN ('pikachu', 'charizard', 'bulbasaur', 'squirtle', 'eevee', 
                                     'mew', 'mewtwo', 'snorlax', 'gengar', 'gyarados') THEN 'S'
            
            -- Other highly recognizable Pokémon
            WHEN LOWER(pokemon_name) IN ('lucario', 'garchomp', 'greninja', 'blaziken', 'gardevoir', 
                                     'rayquaza', 'dragonite', 'jigglypuff', 'mimikyu', 'umbreon', 
                                     'sylveon', 'metagross') THEN 'A'
            
            -- Well-known but less iconic
            WHEN LOWER(pokemon_name) IN ('arcanine', 'tyranitar', 'salamence', 'infernape', 'hydreigon',
                                     'volcarona', 'arceus', 'suicune', 'absol', 'darkrai',
                                     'blastoise', 'venusaur') THEN 'B'
                                     
            -- Pokémon featured prominently in anime/movies
            WHEN LOWER(pokemon_name) IN ('lugia', 'ho-oh', 'celebi', 'jirachi', 'latios', 'latias',
                                     'dialga', 'palkia', 'giratina', 'zoroark', 'victini',
                                     'zekrom', 'reshiram', 'keldeo', 'genesect') THEN 'B'
            
            -- Legendary status boost
            WHEN is_legendary = TRUE THEN 'B'
            WHEN is_mythical = TRUE THEN 'B'
            
            -- Generation popularity bias
            WHEN generation_number = 1 THEN 'C'  -- Original 151 are generally popular
            WHEN generation_number = 2 THEN 'C'
            WHEN generation_number = 3 THEN 'C'
            WHEN generation_number = 4 THEN 'D'
            WHEN generation_number = 5 THEN 'D'
            WHEN generation_number = 6 THEN 'D'
            WHEN generation_number >= 7 THEN 'C'  -- Newer generations get attention
            
            -- Default popularity tier
            ELSE 'E'
        END AS popularity_tier
    FROM base_pokemon
),

-- Evolution chain data
evolution_data AS (
    SELECT 
        pokemon_id,
        CASE
            -- Special case handling for popular evolution chains
            WHEN pokemon_name IN ('Bulbasaur', 'Charmander', 'Squirtle', 'Chikorita', 'Cyndaquil', 'Totodile',
                                'Treecko', 'Torchic', 'Mudkip', 'Turtwig', 'Chimchar', 'Piplup', 'Rowlet', 
                                'Litten', 'Popplio', 'Grookey', 'Scorbunny', 'Sobble', 'Pichu', 'Cleffa', 
                                'Igglybuff', 'Togepi', 'Eevee') THEN 'Basic'
                                
            WHEN pokemon_name IN ('Ivysaur', 'Charmeleon', 'Wartortle', 'Bayleef', 'Quilava', 'Croconaw',
                                'Grovyle', 'Combusken', 'Marshtomp', 'Grotle', 'Monferno', 'Prinplup',
                                'Dartrix', 'Torracat', 'Brionne', 'Thwackey', 'Raboot', 'Drizzile',
                                'Pikachu', 'Clefairy', 'Jigglypuff', 'Togetic') THEN 'Stage 1'
                                
            WHEN pokemon_name IN ('Venusaur', 'Charizard', 'Blastoise', 'Meganium', 'Typhlosion', 'Feraligatr',
                                'Sceptile', 'Blaziken', 'Swampert', 'Torterra', 'Infernape', 'Empoleon',
                                'Decidueye', 'Incineroar', 'Primarina', 'Rillaboom', 'Cinderace', 'Inteleon',
                                'Raichu', 'Clefable', 'Wigglytuff', 'Togekiss') THEN 'Stage 2'
                                
            -- Legendary and mythical Pokémon
            WHEN is_legendary = TRUE OR is_mythical = TRUE THEN 'Legendary/Mythical'
            
            -- Basic determination using stats
            WHEN total_base_stats < 350 THEN 'Basic'
            WHEN total_base_stats < 470 THEN 'Stage 1'
            ELSE 'Stage 2'
        END AS evolution_stage
    FROM base_pokemon
)

SELECT
    -- Primary and foreign keys
    {{ dbt_utils.generate_surrogate_key(['bp.pokemon_id']) }} AS pokemon_key,
    bp.pokemon_id,
    {{ dbt_utils.generate_surrogate_key(['bp.primary_type']) }} AS primary_type_key,
    {{ dbt_utils.generate_surrogate_key(['bp.secondary_type']) }} AS secondary_type_key,
    
    -- Core identifiers
    bp.pokemon_name,
    bp.primary_type,
    bp.secondary_type,
    
    -- Physical attributes with data validation
    COALESCE(bp.height, 0) AS height_m,
    COALESCE(bp.weight, 0) AS weight_kg,
    CASE 
        WHEN bp.height > 0 AND bp.weight > 0 
        THEN ROUND(bp.weight / (bp.height * bp.height), 2)
        ELSE NULL
    END AS weight_height_ratio,
    
    -- Stats with proper handling
    COALESCE(bp.base_stat_hp, 0) AS hp,
    COALESCE(bp.base_stat_attack, 0) AS attack,
    COALESCE(bp.base_stat_defense, 0) AS defense,
    COALESCE(bp.base_stat_special_attack, 0) AS special_attack,
    COALESCE(bp.base_stat_special_defense, 0) AS special_defense,
    COALESCE(bp.base_stat_speed, 0) AS speed,
    COALESCE(bp.total_base_stats, 0) AS total_stats,
    
    -- Attribute counts
    COALESCE(ac.ability_count, 0) AS ability_count,
    COALESCE(ac.move_count, 0) AS move_count,
    
    -- Categorical classifications
    bp.generation_number,
    COALESCE(bp.is_legendary, FALSE) AS is_legendary,
    COALESCE(bp.is_mythical, FALSE) AS is_mythical,
    
    -- Gameplay attributes
    bp.base_xp,
    COALESCE(bp.capture_rate, 0) AS capture_rate,
    COALESCE(bp.base_happiness, 0) AS base_happiness,
    bp.evolution_chain_id,
    bp.species_id,
    
    -- Derived evolutionary data
    ed.evolution_stage,
    
    -- Enhanced classifications
    CASE
        WHEN bp.is_legendary THEN 'Legendary'
        WHEN bp.is_mythical THEN 'Mythical'
        WHEN ed.evolution_stage = 'Stage 2' THEN 'Fully Evolved'
        WHEN ed.evolution_stage = 'Stage 1' THEN 'Mid Evolution'
        WHEN ed.evolution_stage = 'Basic' THEN 'Basic Form'
        ELSE 'Unknown'
    END AS pokemon_class,
    
    -- Stat-based classifications
    CASE
        WHEN bp.total_base_stats >= 600 THEN 'Pseudo-Legendary'
        WHEN bp.total_base_stats >= 500 THEN 'Strong'
        WHEN bp.total_base_stats >= 400 THEN 'Average'
        WHEN bp.total_base_stats >= 300 THEN 'Basic'
        ELSE 'Weak'
    END AS strength_tier,
    
    -- Battle role classification
    CASE
        WHEN bp.base_stat_speed >= 100 AND 
             (bp.base_stat_attack >= 100 OR bp.base_stat_special_attack >= 100) THEN 'Fast Attacker'
        WHEN bp.base_stat_hp >= 100 AND 
             (bp.base_stat_defense >= 100 OR bp.base_stat_special_defense >= 100) THEN 'Tank'
        WHEN bp.base_stat_attack >= 120 THEN 'Physical Sweeper'
        WHEN bp.base_stat_special_attack >= 120 THEN 'Special Sweeper'
        WHEN bp.base_stat_defense >= 120 THEN 'Physical Wall'
        WHEN bp.base_stat_special_defense >= 120 THEN 'Special Wall'
        WHEN bp.base_stat_hp >= 120 THEN 'Bulky'
        WHEN bp.base_stat_speed >= 120 THEN 'Speedy'
        WHEN (bp.base_stat_attack + bp.base_stat_special_attack) > 
             (bp.base_stat_defense + bp.base_stat_special_defense) THEN 'Offensive'
        WHEN (bp.base_stat_defense + bp.base_stat_special_defense) > 
             (bp.base_stat_attack + bp.base_stat_special_attack) THEN 'Defensive'
        ELSE 'Balanced'
    END AS battle_style,
    
    -- Popularity data
    p.popularity_tier,
    CASE 
        WHEN p.popularity_tier = 'S' THEN 'Mascot/Icon'
        WHEN p.popularity_tier = 'A' THEN 'Fan Favorite'
        WHEN p.popularity_tier = 'B' THEN 'Popular'
        WHEN p.popularity_tier = 'C' THEN 'Well Known'
        WHEN p.popularity_tier = 'D' THEN 'Recognized'
        ELSE 'Standard'
    END AS popularity_class,
    
    -- Calculated metrics
    ROUND((bp.base_stat_attack + bp.base_stat_special_attack) / 
          NULLIF((bp.base_stat_defense + bp.base_stat_special_defense), 0), 2) AS offense_defense_ratio,
    
    CASE
        WHEN bp.base_stat_attack > bp.base_stat_special_attack THEN 'Physical'
        WHEN bp.base_stat_special_attack > bp.base_stat_attack THEN 'Special'
        ELSE 'Mixed'
    END AS attack_bias,
    
    -- Stat distribution percentages
    ROUND(100.0 * bp.base_stat_hp / NULLIF(bp.total_base_stats, 0), 1) AS hp_percent,
    ROUND(100.0 * (bp.base_stat_attack + bp.base_stat_special_attack) / NULLIF(bp.total_base_stats, 0), 1) AS attack_percent,
    ROUND(100.0 * (bp.base_stat_defense + bp.base_stat_special_defense) / NULLIF(bp.total_base_stats, 0), 1) AS defense_percent,
    ROUND(100.0 * bp.base_stat_speed / NULLIF(bp.total_base_stats, 0), 1) AS speed_percent,
    
    -- Data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM base_pokemon bp
JOIN attribute_counts ac ON bp.pokemon_id = ac.pokemon_id
JOIN popularity p ON bp.pokemon_id = p.pokemon_id
JOIN evolution_data ed ON bp.pokemon_id = ed.pokemon_id
WHERE bp.pokemon_id IS NOT NULL
ORDER BY bp.total_base_stats DESC