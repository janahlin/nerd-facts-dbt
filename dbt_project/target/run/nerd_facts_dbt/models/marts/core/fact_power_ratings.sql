
  
    

  create  table "nerd_facts"."public"."fact_power_ratings__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: fact_power_ratings (Ultra-Simplified)
  Description: Basic power metrics across universes with consistent types
*/

WITH sw_power AS (
    SELECT
        md5(cast(coalesce(cast('star_wars' as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(p.people_id::TEXT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
        'star_wars' AS universe,
        p.people_id::TEXT AS character_source_id,
        p.name AS character_name,
        1 AS base_power,  -- Simplified constant
        1 AS mobility,    -- Simplified constant
        1 AS battle_experience,  -- Simplified constant
        'false' AS has_special_abilities  -- String literal instead of boolean expression
    FROM "nerd_facts"."public"."stg_swapi_people" p
),

pokemon_power AS (
    SELECT
        md5(cast(coalesce(cast('pokemon' as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(p.pokemon_id::TEXT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
        'pokemon' AS universe,
        p.pokemon_id::TEXT AS character_source_id,
        p.pokemon_name AS character_name,
        1 AS base_power,  -- Simplified constant
        1 AS mobility,    -- Simplified constant
        1 AS battle_experience,  -- Simplified constant
        'true' AS has_special_abilities  -- String literal instead of boolean
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p
),

netrunner_power AS (
    SELECT
        md5(cast(coalesce(cast('netrunner' as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(c.card_id::TEXT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
        'netrunner' AS universe,
        c.code AS character_source_id,
        c.card_name AS character_name,
        1 AS base_power,  -- Simplified constant
        1 AS mobility,    -- Simplified constant
        1 AS battle_experience,  -- Simplified constant
        CASE 
            WHEN c.uniqueness IS NULL THEN 'false'
            -- Compare as text instead of using as boolean
            WHEN c.uniqueness = 'true' OR c.uniqueness = 't' OR c.uniqueness = '1' THEN 'true' 
            ELSE 'false' 
        END AS has_special_abilities
    FROM "nerd_facts"."public"."stg_netrunner_cards" c
    WHERE c.type_code = 'identity'  -- Only include identity cards
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
  );
  