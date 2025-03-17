/*
  Model: fct_power_ratings (Ultra-Simplified)
  Description: Basic power metrics across universes with consistent types
*/

WITH sw_power AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["'star_wars'", 'p.id::TEXT']) }} AS character_key,
        'star_wars' AS universe,
        p.id::TEXT AS character_source_id,
        p.name AS character_name,
        1 AS base_power,  -- Simplified constant
        1 AS mobility,    -- Simplified constant
        1 AS battle_experience,  -- Simplified constant
        CASE WHEN p.force_sensitive THEN 'true' ELSE 'false' END AS has_special_abilities  -- Use CASE instead of COALESCE
    FROM {{ ref('stg_swapi_people') }} p
),

pokemon_power AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["'pokemon'", 'p.id::TEXT']) }} AS character_key,
        'pokemon' AS universe,
        p.id::TEXT AS character_source_id,
        p.name AS character_name,
        1 AS base_power,  -- Simplified constant
        1 AS mobility,    -- Simplified constant
        1 AS battle_experience,  -- Simplified constant
        'true' AS has_special_abilities  -- String literal instead of boolean
    FROM {{ ref('stg_pokeapi_pokemon') }} p
),

netrunner_power AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["'netrunner'", 'c.code']) }} AS character_key,
        'netrunner' AS universe,
        c.code AS character_source_id,
        c.card_name AS character_name,
        1 AS base_power,  -- Simplified constant
        1 AS mobility,    -- Simplified constant
        1 AS battle_experience,  -- Simplified constant
        CASE 
            WHEN c.is_unique_card IS NULL THEN 'false'
            -- Compare as text instead of using as boolean
            WHEN c.is_unique_card = 'true' OR c.is_unique_card = 't' OR c.is_unique_card = '1' THEN 'true' 
            ELSE 'false' 
        END AS has_special_abilities
    FROM {{ ref('stg_netrunner_cards') }} c
    WHERE c.type_name = 'Identity'  -- Only include identity cards
)

-- Simply combine all data with minimal processing
SELECT
    character_key,
    universe,
    character_source_id,
    character_name,
    base_power,
    mobility,
    battle_experience,
    has_special_abilities,
    
    -- Simple power score (all equal for now)
    3 AS normalized_power_score,
    
    -- Simplified tier (all equal)
    'C-Tier' AS power_tier,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM (
    SELECT * FROM sw_power
    UNION ALL
    SELECT * FROM pokemon_power
    UNION ALL
    SELECT * FROM netrunner_power
) AS combined_power