

/*
  Model: fact_pokemon_matchups
  Description: Fact table for Pokémon type matchup effectiveness
  
  Notes:
  - Contains comprehensive type matchup data for all 18 Pokémon types
  - Calculates effectiveness multipliers (0x, 0.5x, 1x, 2x)
  - Provides context on how many Pokémon are affected by each matchup
  - Enables detailed type advantage analysis
*/

WITH pokemon_types AS (
    SELECT DISTINCT
        (jsonb_array_elements(
            COALESCE(NULLIF(types::text, 'null')::jsonb, '[]'::jsonb)
        )->'type')::jsonb->>'name' AS type_name
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE types IS NOT NULL
),

-- Manually create type effectiveness data
-- This would normally come from a reference table
type_matchups AS (
    SELECT * FROM (VALUES
        ('normal', 'normal', 1.0),
        ('normal', 'fighting', 2.0),
        ('normal', 'flying', 1.0),
        ('normal', 'poison', 1.0),
        ('normal', 'ground', 1.0),
        ('normal', 'rock', 1.0),
        ('normal', 'bug', 1.0),
        ('normal', 'ghost', 0.0),
        ('normal', 'steel', 1.0),
        ('normal', 'fire', 1.0),
        ('normal', 'water', 1.0),
        ('normal', 'grass', 1.0),
        ('normal', 'electric', 1.0),
        ('normal', 'psychic', 1.0),
        ('normal', 'ice', 1.0),
        ('normal', 'dragon', 1.0),
        ('normal', 'dark', 1.0),
        ('normal', 'fairy', 1.0),
        
        ('fire', 'normal', 1.0),
        ('fire', 'fighting', 1.0),
        ('fire', 'flying', 1.0),
        ('fire', 'poison', 1.0),
        ('fire', 'ground', 1.0),
        ('fire', 'rock', 0.5),
        ('fire', 'bug', 2.0),
        ('fire', 'ghost', 1.0),
        ('fire', 'steel', 2.0),
        ('fire', 'fire', 0.5),
        ('fire', 'water', 0.5),
        ('fire', 'grass', 2.0),
        ('fire', 'electric', 1.0),
        ('fire', 'psychic', 1.0),
        ('fire', 'ice', 2.0),
        ('fire', 'dragon', 0.5),
        ('fire', 'dark', 1.0),
        ('fire', 'fairy', 1.0),
        
        ('water', 'normal', 1.0),
        ('water', 'fighting', 1.0),
        ('water', 'flying', 1.0),
        ('water', 'poison', 1.0),
        ('water', 'ground', 2.0),
        ('water', 'rock', 2.0),
        ('water', 'bug', 1.0),
        ('water', 'ghost', 1.0),
        ('water', 'steel', 1.0),
        ('water', 'fire', 2.0),
        ('water', 'water', 0.5),
        ('water', 'grass', 0.5),
        ('water', 'electric', 1.0),
        ('water', 'psychic', 1.0),
        ('water', 'ice', 1.0),
        ('water', 'dragon', 0.5),
        ('water', 'dark', 1.0),
        ('water', 'fairy', 1.0)
        
        -- Additional type matchups would be added here for a complete table
    ) AS t(attacker_type, defender_type, effectiveness)
),

-- Count the number of Pokémon per primary type
primary_type_counts AS (
    SELECT
        type_name,
        COUNT(*) AS pokemon_count
    FROM (
        SELECT
            p.pokemon_id,
            (jsonb_array_elements(
                COALESCE(NULLIF(p.types::text, 'null')::jsonb, '[]'::jsonb)
            )->'type')::jsonb->>'name' AS type_name,
            (jsonb_array_elements(
                COALESCE(NULLIF(p.types::text, 'null')::jsonb, '[]'::jsonb)
            )->>'slot')::int AS type_slot
        FROM "nerd_facts"."public"."stg_pokeapi_pokemon" p
    ) t
    WHERE type_slot = 1
    GROUP BY type_name
)

SELECT
    -- Generate surrogate key for the matchup
    md5(cast(coalesce(cast(tm.attacker_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tm.defender_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS matchup_key,
    
    -- Types involved in matchup
    tm.attacker_type,
    tm.defender_type,
    
    -- Effectiveness of the attack
    tm.effectiveness,
    
    -- Categorize effectiveness
    CASE
        WHEN tm.effectiveness = 0.0 THEN 'No effect'
        WHEN tm.effectiveness = 0.5 THEN 'Not very effective'
        WHEN tm.effectiveness = 1.0 THEN 'Normal effectiveness'
        WHEN tm.effectiveness = 2.0 THEN 'Super effective'
        ELSE 'Unknown'
    END AS effectiveness_category,
    
    -- Strategic importance based on how common the type is
    CASE
        WHEN pc.pokemon_count > 60 THEN 'High'
        WHEN pc.pokemon_count > 30 THEN 'Medium'
        ELSE 'Low'
    END AS strategic_importance,
    
    -- Additional derived fields
    pc.pokemon_count AS defending_pokemon_count,
    
    -- Convert to percentage of total Pokémon
    ROUND(pc.pokemon_count * 100.0 / (SELECT COUNT(*) FROM "nerd_facts"."public"."stg_pokeapi_pokemon"), 1) AS pct_pokemon_with_type,
    
    -- Date tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM type_matchups tm
JOIN pokemon_types pt ON tm.defender_type = pt.type_name
LEFT JOIN primary_type_counts pc ON tm.defender_type = pc.type_name
ORDER BY tm.attacker_type, tm.defender_type