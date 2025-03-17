/*
  Model: fct_power_ratings
  Description: Cross-universe fact table for character power metrics and comparisons
  
  Notes:
  - Combines power metrics from Star Wars, Pokémon, and Netrunner universes
  - Standardizes metrics across universes using common dimensions
  - Creates normalized power scores for cross-universe comparisons
  - Includes universe-specific attributes while maintaining consistent schema
  
  Power Score Calculation:
  - base_power (60%): Raw power/strength of the character
  - mobility (20%): Speed and movement capabilities
  - battle_experience (20%): Combat experience and victories
  
  The final score is scaled to 0-100 for easy comparison
*/

WITH sw_power AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'star_wars\'', 'c.character_source_id']) }} AS character_key,
        'star_wars' AS universe,
        p.id AS character_source_id,
        p.name AS character_name,
        CASE
            WHEN COALESCE(p.force_sensitive, false) THEN 
                5 * COALESCE(p.force_rating, 1)
            ELSE 1
        END AS base_power,
        COALESCE(p.ships_piloted, 0) AS mobility,
        COALESCE(p.film_appearances, 0) * 2 AS battle_experience,
        p.force_sensitive AS has_special_abilities
    FROM {{ ref('stg_swapi_people') }} p
),

pokemon_power AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'pokemon\'', 'p.id::VARCHAR']) }} AS character_key,
        'pokemon' AS universe,
        p.id::VARCHAR AS character_source_id,
        p.name AS character_name,
        -- Use average of attack and special attack for balanced rating
        (COALESCE(p.base_stat_attack, 0) + COALESCE(p.base_stat_special_attack, 0)) / 2 AS base_power,
        COALESCE(p.base_stat_speed, 0) AS mobility,
        -- Use generation as a proxy for battle experience (older = more experienced)
        COALESCE(10 - p.generation_number, 0) * 5 AS battle_experience,
        TRUE AS has_special_abilities -- All Pokémon have abilities
    FROM {{ ref('stg_pokeapi_pokemon') }} p
),

netrunner_power AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'netrunner\'', 'c.code']) }} AS character_key,
        'netrunner' AS universe,
        c.code AS character_source_id,
        c.card_name AS character_name,
        -- For Netrunner: base power is influence limit for identities
        COALESCE(c.influence_limit, 0) * 2 AS base_power,
        CASE
            WHEN c.memory_cost IS NOT NULL THEN COALESCE(c.memory_cost, 0) * 5
            ELSE 10 -- Default mobility
        END AS mobility,
        -- Use card cycle as proxy for battle experience
        CASE
            WHEN p.cycle_code IN ('core', 'genesis') THEN 40
            WHEN p.cycle_code IN ('creation-and-control', 'spin') THEN 30
            WHEN p.cycle_code IN ('lunar', 'sansan', 'mumbad') THEN 20
            ELSE 10
        END AS battle_experience,
        c.is_unique AS has_special_abilities
    FROM {{ ref('stg_netrunner_cards') }} c
    LEFT JOIN {{ ref('stg_netrunner_packs') }} p ON c.pack_code = p.code
    WHERE c.type_code = 'identity' -- Only include identity cards
),

combined_power AS (
    SELECT * FROM sw_power
    UNION ALL
    SELECT * FROM pokemon_power
    UNION ALL
    SELECT * FROM netrunner_power
)

SELECT
    character_key,
    universe,
    character_source_id,
    character_name,
    base_power,
    mobility,
    battle_experience,
    has_special_abilities,
    
    -- Normalized power score (0-100 scale)
    GREATEST(0, LEAST(100, 
        ROUND((base_power * 0.6) + (mobility * 0.2) + (battle_experience * 0.2))
    )) AS normalized_power_score,
    
    -- Power tier classification
    CASE
        WHEN (base_power * 0.6) + (mobility * 0.2) + (battle_experience * 0.2) >= 80 THEN 'S-Tier'
        WHEN (base_power * 0.6) + (mobility * 0.2) + (battle_experience * 0.2) >= 60 THEN 'A-Tier'
        WHEN (base_power * 0.6) + (mobility * 0.2) + (battle_experience * 0.2) >= 40 THEN 'B-Tier'
        WHEN (base_power * 0.6) + (mobility * 0.2) + (battle_experience * 0.2) >= 20 THEN 'C-Tier'
        ELSE 'D-Tier'
    END AS power_tier,
    
    -- Calculate universe-relative percentile (how powerful within their own universe)
    PERCENT_RANK() OVER(PARTITION BY universe ORDER BY 
        (base_power * 0.6) + (mobility * 0.2) + (battle_experience * 0.2)
    ) AS universe_power_percentile,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM combined_power