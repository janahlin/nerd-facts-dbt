{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['pokemon_id']}, {'columns': ['primary_type']}],
    unique_key = 'pokemon_stat_id'
  )
}}

/*
  Model: fct_pokemon_stats
  Description: Fact table for Pokémon statistics and battle metrics
  
  Notes:
  - Contains comprehensive stat analysis for all Pokémon
  - Calculates evolutionary stage and progression metrics
  - Includes battle effectiveness calculations and classifications
  - Provides stat distribution analysis and percentile rankings
  - Links to dimension tables for Pokémon and types
*/

WITH base_pokemon AS (
    SELECT
        id,
        name,
        primary_type,
        secondary_type,
        generation_number,
        is_legendary,
        is_mythical,
        height,
        weight,
        base_xp,
        -- Include all base stats
        base_stat_hp,
        base_stat_attack,
        base_stat_defense,
        base_stat_special_attack,
        base_stat_special_defense,
        base_stat_speed,
        total_base_stats,
        capture_rate,
        base_happiness,
        evolution_chain_id
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE id IS NOT NULL
),

-- Calculate stat percentiles across all Pokémon
stat_percentiles AS (
    SELECT
        id,
        PERCENT_RANK() OVER (ORDER BY base_stat_hp) AS hp_percentile,
        PERCENT_RANK() OVER (ORDER BY base_stat_attack) AS attack_percentile,
        PERCENT_RANK() OVER (ORDER BY base_stat_defense) AS defense_percentile,
        PERCENT_RANK() OVER (ORDER BY base_stat_special_attack) AS sp_attack_percentile,
        PERCENT_RANK() OVER (ORDER BY base_stat_special_defense) AS sp_defense_percentile,
        PERCENT_RANK() OVER (ORDER BY base_stat_speed) AS speed_percentile,
        PERCENT_RANK() OVER (ORDER BY total_base_stats) AS total_stats_percentile
    FROM base_pokemon
),

-- Improved evolutionary stage determination
evolution_stage AS (
    SELECT
        bp.id,
        CASE
            -- Use evolution chain data if available and reliable
            WHEN bp.evolution_chain_id IS NOT NULL THEN
                CASE
                    -- Known starter Pokémon evolution patterns
                    WHEN bp.name IN ('Bulbasaur', 'Charmander', 'Squirtle', 'Chikorita', 
                                    'Cyndaquil', 'Totodile', 'Treecko', 'Torchic', 
                                    'Mudkip', 'Turtwig', 'Chimchar', 'Piplup') THEN 1
                    WHEN bp.name IN ('Ivysaur', 'Charmeleon', 'Wartortle', 'Bayleef', 
                                    'Quilava', 'Croconaw', 'Grovyle', 'Combusken',
                                    'Marshtomp', 'Grotle', 'Monferno', 'Prinplup') THEN 2
                    WHEN bp.name IN ('Venusaur', 'Charizard', 'Blastoise', 'Meganium',
                                    'Typhlosion', 'Feraligatr', 'Sceptile', 'Blaziken',
                                    'Swampert', 'Torterra', 'Infernape', 'Empoleon') THEN 3
                                    
                    -- Legendaries and Mythicals are typically final forms
                    WHEN bp.is_legendary = TRUE OR bp.is_mythical = TRUE THEN 3
                    
                    -- Use base stats as a proxy for evolution stage
                    WHEN bp.total_base_stats < 350 THEN 1  -- Basic form
                    WHEN bp.total_base_stats < 470 THEN 2  -- First evolution
                    ELSE 3  -- Final evolution
                END
            ELSE
                -- Fallback if no evolution chain data
                CASE
                    WHEN bp.total_base_stats < 350 THEN 1  -- Basic form
                    WHEN bp.total_base_stats < 470 THEN 2  -- First evolution
                    ELSE 3  -- Final evolution
                END
        END AS evolution_level
    FROM base_pokemon bp
),

-- Enhanced battle metrics with improved calculations
battle_metrics AS (
    SELECT
        p.id,
        -- Calculate victories with expanded logic
        CASE 
            -- Legendaries and mythicals
            WHEN p.is_legendary OR p.is_mythical THEN 
                20 + FLOOR(p.total_base_stats / 100)
                
            -- Well-known powerful Pokémon
            WHEN LOWER(p.name) IN ('mewtwo', 'dragonite', 'tyranitar', 'salamence', 'garchomp',
                                'volcarona', 'hydreigon', 'dragapult', 'heatran', 'chansey',
                                'blissey', 'snorlax', 'ferrothorn', 'toxapex', 'gliscor',
                                'landorus', 'thundurus', 'tornadus', 'rotom-wash') THEN 
                15 + FLOOR(p.total_base_stats / 150)
                
            -- Popular starters and favorites
            WHEN LOWER(p.name) IN ('charizard', 'blastoise', 'venusaur', 'gyarados', 
                                'arcanine', 'gengar', 'alakazam', 'lucario', 'togekiss',
                                'gardevoir', 'metagross', 'scizor', 'excadrill', 'sylveon') THEN 
                10 + FLOOR(p.total_base_stats / 200)
                
            -- Base formula for other Pokémon with enhanced factors
            ELSE 
                GREATEST(
                    FLOOR(
                        (COALESCE(p.base_xp, 100) / 60) + 
                        (COALESCE(p.total_base_stats, 300) / 100) + 
                        e.evolution_level + 
                        (CASE WHEN p.generation_number <= 3 THEN 2 ELSE 0 END)  -- Bonus for older generations
                    ),
                    1  -- Minimum 1 victory
                )
        END AS estimated_victories,
        
        -- Enhanced combat classification with more nuanced roles
        CASE
            -- Special sweeper
            WHEN p.base_stat_special_attack > 110 AND p.base_stat_speed >= 90 THEN 'Special Sweeper'
            
            -- Physical sweeper
            WHEN p.base_stat_attack > 110 AND p.base_stat_speed >= 90 THEN 'Physical Sweeper'
            
            -- Special wall
            WHEN p.base_stat_special_defense > 110 AND p.base_stat_hp >= 80 THEN 'Special Wall'
            
            -- Physical wall
            WHEN p.base_stat_defense > 110 AND p.base_stat_hp >= 80 THEN 'Physical Wall'
            
            -- All-around tank
            WHEN p.base_stat_defense > 90 AND p.base_stat_special_defense > 90 AND p.base_stat_hp >= 80 THEN 'Tank'
            
            -- Support/utility
            WHEN p.base_stat_hp > 90 AND p.base_stat_speed > 70 AND 
                 p.base_stat_attack < 80 AND p.base_stat_special_attack < 80 THEN 'Support'
            
            -- Fast attacker
            WHEN p.base_stat_speed > 100 THEN 'Fast Attacker'
            
            -- Physical attacker
            WHEN p.base_stat_attack > p.base_stat_special_attack + 20 THEN 'Physical Attacker'
            
            -- Special attacker
            WHEN p.base_stat_special_attack > p.base_stat_attack + 20 THEN 'Special Attacker'
            
            -- Bulky attacker
            WHEN (p.base_stat_attack > 90 OR p.base_stat_special_attack > 90) AND p.base_stat_hp > 80 THEN 'Bulky Attacker'
            
            -- Default balanced
            ELSE 'Balanced'
        END AS battle_role,
        
        -- Offensive/defensive balance on a scale
        CASE
            WHEN (p.base_stat_attack + p.base_stat_special_attack) > 
                 (p.base_stat_defense + p.base_stat_special_defense + p.base_stat_hp) * 1.5 THEN 'Extremely Offensive'
            WHEN (p.base_stat_attack + p.base_stat_special_attack) > 
                 (p.base_stat_defense + p.base_stat_special_defense + p.base_stat_hp) * 1.2 THEN 'Offensive'
            WHEN (p.base_stat_defense + p.base_stat_special_defense + p.base_stat_hp) > 
                 (p.base_stat_attack + p.base_stat_special_attack) * 1.5 THEN 'Extremely Defensive'
            WHEN (p.base_stat_defense + p.base_stat_special_defense + p.base_stat_hp) > 
                 (p.base_stat_attack + p.base_stat_special_attack) * 1.2 THEN 'Defensive'
            ELSE 'Balanced'
        END AS offensive_defensive_balance,
        
        -- Attack style preference
        CASE 
            WHEN p.base_stat_attack > p.base_stat_special_attack * 1.5 THEN 'Strongly Physical'
            WHEN p.base_stat_attack > p.base_stat_special_attack * 1.2 THEN 'Physical'
            WHEN p.base_stat_special_attack > p.base_stat_attack * 1.5 THEN 'Strongly Special'
            WHEN p.base_stat_special_attack > p.base_stat_attack * 1.2 THEN 'Special'
            ELSE 'Mixed'
        END AS attack_style
    FROM base_pokemon p
    JOIN evolution_stage e ON p.id = e.id
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['bp.id']) }} AS pokemon_stat_id,
    
    -- Foreign keys
    {{ dbt_utils.generate_surrogate_key(['bp.id']) }} AS pokemon_key,
    {{ dbt_utils.generate_surrogate_key(['bp.primary_type']) }} AS primary_type_key,
    {{ dbt_utils.generate_surrogate_key(['bp.secondary_type']) }} AS secondary_type_key,
    
    -- Core identifiers
    bp.id AS pokemon_id,
    bp.name AS pokemon_name,
    bp.primary_type,
    bp.secondary_type,
    
    -- Physical attributes
    bp.height,
    bp.weight,
    ROUND(bp.weight / NULLIF(POWER(bp.height, 2), 0), 2) AS bmi,  -- Body Mass Index equivalent
    
    -- Basic stats
    bp.base_stat_hp,
    bp.base_stat_attack,
    bp.base_stat_defense,
    bp.base_stat_special_attack,
    bp.base_stat_special_defense,
    bp.base_stat_speed,
    bp.total_base_stats,
    
    -- Stat percentiles for comparative analysis
    ROUND(sp.hp_percentile * 100) AS hp_percentile,
    ROUND(sp.attack_percentile * 100) AS attack_percentile,
    ROUND(sp.defense_percentile * 100) AS defense_percentile,
    ROUND(sp.sp_attack_percentile * 100) AS sp_attack_percentile,
    ROUND(sp.sp_defense_percentile * 100) AS sp_defense_percentile,
    ROUND(sp.speed_percentile * 100) AS speed_percentile,
    ROUND(sp.total_stats_percentile * 100) AS total_stats_percentile,
    
    -- Statistical tier based on total stats
    CASE
        WHEN sp.total_stats_percentile >= 0.95 THEN 'S+'  -- Top 5%
        WHEN sp.total_stats_percentile >= 0.90 THEN 'S'   -- Top 10%
        WHEN sp.total_stats_percentile >= 0.80 THEN 'A+'  -- Top 20%
        WHEN sp.total_stats_percentile >= 0.70 THEN 'A'   -- Top 30%
        WHEN sp.total_stats_percentile >= 0.60 THEN 'B+'  -- Top 40%
        WHEN sp.total_stats_percentile >= 0.50 THEN 'B'   -- Top 50%
        WHEN sp.total_stats_percentile >= 0.35 THEN 'C'   -- Top 65%
        WHEN sp.total_stats_percentile >= 0.20 THEN 'D'   -- Top 80%
        ELSE 'E'                                         -- Bottom 20%
    END AS stat_tier,
    
    -- Evolutionary data
    es.evolution_level,
    bp.evolution_chain_id,
    
    -- Battle metrics
    bm.estimated_victories,
    bm.battle_role,
    bm.offensive_defensive_balance,
    bm.attack_style,
    
    -- Enhanced type effectiveness
    CASE 
        WHEN bp.primary_type IN ('Steel', 'Ghost', 'Fairy') OR 
             bp.secondary_type IN ('Steel', 'Ghost', 'Fairy') THEN 'High'
        WHEN bp.primary_type IN ('Dragon', 'Dark', 'Water', 'Flying') OR 
             bp.secondary_type IN ('Dragon', 'Dark', 'Water', 'Flying') THEN 'Medium'
        ELSE 'Standard'
    END AS defensive_typing_quality,
    
    -- Stat distribution
    ROUND(bp.base_stat_hp::NUMERIC / NULLIF(bp.total_base_stats, 0) * 100, 1) AS hp_percentage,
    ROUND((bp.base_stat_attack + bp.base_stat_special_attack)::NUMERIC / NULLIF(bp.total_base_stats, 0) * 100, 1) AS offensive_percentage,
    ROUND((bp.base_stat_defense + bp.base_stat_special_defense)::NUMERIC / NULLIF(bp.total_base_stats, 0) * 100, 1) AS defensive_percentage,
    ROUND(bp.base_stat_speed::NUMERIC / NULLIF(bp.total_base_stats, 0) * 100, 1) AS speed_percentage,
    
    -- Win rate calculation with enhanced logic
    CASE
        WHEN bp.is_legendary OR bp.is_mythical THEN 
            GREATEST(LEAST(0.7 + (sp.total_stats_percentile * 0.25), 0.98), 0.7)  -- 70-98%
        WHEN bm.estimated_victories > 10 THEN
            GREATEST(LEAST(0.5 + (bm.estimated_victories / 40.0), 0.9), 0.5)  -- 50-90%
        WHEN sp.total_stats_percentile > 0.8 THEN 
            GREATEST(LEAST(0.6 + (sp.total_stats_percentile * 0.2), 0.85), 0.6)  -- 60-85%
        ELSE 
            GREATEST(LEAST(0.4 + (sp.total_stats_percentile * 0.3), 0.7), 0.4)  -- 40-70%
    END AS estimated_win_rate,
    
    -- Additional pokemon attributes
    bp.base_xp,
    bp.capture_rate,
    bp.base_happiness,
    bp.generation_number,
    bp.is_legendary,
    bp.is_mythical,
    
    -- Competitive viability score (1-100)
    GREATEST(LEAST(
        (bp.total_base_stats / 7) +  -- Up to ~85 points from stats
        (CASE WHEN bp.is_legendary OR bp.is_mythical THEN 10 ELSE 0 END) +  -- Legendary bonus
        (CASE WHEN bm.battle_role IN ('Special Sweeper', 'Physical Sweeper', 'Tank') THEN 5 ELSE 0 END) +  -- Role bonus
        (CASE WHEN bp.generation_number >= 5 THEN 3 ELSE 0 END) +  -- Recent generation bonus
        (CASE WHEN bp.primary_type IN ('Fairy', 'Steel', 'Dragon', 'Ground', 'Ghost') OR
               bp.secondary_type IN ('Fairy', 'Steel', 'Dragon', 'Ground', 'Ghost') THEN 7 ELSE 0 END)  -- Type bonus
    , 100), 1) AS competitive_viability_score,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM base_pokemon bp
JOIN stat_percentiles sp ON bp.id = sp.id
JOIN evolution_stage es ON bp.id = es.id
JOIN battle_metrics bm ON bp.id = bm.id
ORDER BY bp.total_base_stats DESC