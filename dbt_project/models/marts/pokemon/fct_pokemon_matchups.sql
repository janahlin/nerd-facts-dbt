{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['matchup_id']}, {'columns': ['attacking_type', 'defending_type']}],
    unique_key = 'matchup_id'
  )
}}

/*
  Model: fct_pokemon_matchups
  Description: Fact table for Pokémon type matchup effectiveness
  
  Notes:
  - Contains comprehensive type matchup data for all 18 Pokémon types
  - Calculates effectiveness multipliers (0x, 0.5x, 1x, 2x)
  - Provides context on how many Pokémon are affected by each matchup
  - Includes dual-type effectiveness calculations
  - Creates detailed descriptions for analysis and visualization
*/

WITH type_pairs AS (
    -- Generate all possible type combinations
    SELECT
        a.type_name AS attacking_type,
        d.type_name AS defending_type
    FROM {{ ref('dim_pokemon_types') }} a
    CROSS JOIN {{ ref('dim_pokemon_types') }} d
    WHERE a.type_name != 'Unknown' AND d.type_name != 'Unknown' -- Filter out Unknown type
),

-- Use the arrays from dim_pokemon_types to determine effectiveness
type_effectiveness AS (
    SELECT
        tp.attacking_type,
        tp.defending_type,
        
        -- Determine effectiveness by checking immune/resistant/weak arrays
        CASE
            -- No effect (0x damage) if defending type is immune to attacking type
            WHEN tp.defending_type IN (
                SELECT unnest(immunities) 
                FROM {{ ref('dim_pokemon_types') }} 
                WHERE type_name = tp.attacking_type
            ) THEN 0
            
            -- Super effective (2x damage) if defending type is weak to attacking type
            WHEN tp.defending_type IN (
                SELECT unnest(super_effective) 
                FROM {{ ref('dim_pokemon_types') }} 
                WHERE type_name = tp.attacking_type
            ) THEN 2
            
            -- Not very effective (0.5x damage) if defending type resists attacking type
            WHEN tp.defending_type IN (
                SELECT unnest(resistances) 
                FROM {{ ref('dim_pokemon_types') }} 
                WHERE type_name = tp.attacking_type
            ) THEN 0.5
            
            -- Normal effectiveness (1x damage) for everything else
            ELSE 1
        END AS effectiveness_multiplier
    FROM type_pairs tp
),

-- Count Pokémon with each primary and secondary type combination
pokemon_type_counts AS (
    SELECT
        type_list[0] AS primary_type,
        type_list[1] AS secondary_type,  -- Get secondary type from array
        COUNT(*) AS pokemon_count
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE type_list[0] IS NOT NULL  -- Only include Pokémon with at least one type
    GROUP BY type_list[0], type_list[1]
),

-- Get dual-type defending Pokémon and calculate combined effectiveness
dual_type_effectiveness AS (
    SELECT
        te1.attacking_type,
        ptc.primary_type AS primary_defending_type,
        ptc.secondary_type AS secondary_defending_type,
        -- Multiply effectiveness against both types
        te1.effectiveness_multiplier * te2.effectiveness_multiplier AS combined_effectiveness,
        ptc.pokemon_count
    FROM pokemon_type_counts ptc
    JOIN type_effectiveness te1 ON ptc.primary_type = te1.defending_type
    JOIN type_effectiveness te2 ON ptc.secondary_type = te2.defending_type
    WHERE ptc.secondary_type IS NOT NULL  -- Only include dual-type Pokémon
),

-- Calculate type usage statistics
type_usage_stats AS (
    SELECT
        defending_type,
        SUM(pokemon_count) as type_pokemon_count
    FROM (
        -- Count primary type usage
        SELECT primary_type as defending_type, COUNT(*) as pokemon_count
        FROM {{ ref('stg_pokeapi_pokemon') }}
        WHERE type_list[0] IS NOT NULL
        GROUP BY primary_type
        UNION ALL
        -- Count secondary type usage
        SELECT type_list[1] as defending_type, COUNT(*) as pokemon_count
        FROM {{ ref('stg_pokeapi_pokemon') }}
        WHERE type_list[1] IS NOT NULL
        GROUP BY type_list[1]
    ) t
    GROUP BY defending_type
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['te.attacking_type', 'te.defending_type']) }} AS matchup_id,
    
    -- Core dimensions
    te.attacking_type,
    te.defending_type,
    te.effectiveness_multiplier,
    
    -- Attacking type metadata
    at.type_category AS attacking_type_category,
    at.damage_category AS attacking_damage_category,
    at.type_color AS attacking_type_color,
    
    -- Defending type metadata
    dt.type_category AS defending_type_category,
    dt.resistance_count,
    dt.weakness_count,
    dt.immunity_count,
    dt.type_color AS defending_type_color,
    
    -- Effectiveness category with more detail
    CASE
        WHEN te.effectiveness_multiplier = 0 THEN 'Immune (0x)'
        WHEN te.effectiveness_multiplier = 0.25 THEN 'Doubly resistant (0.25x)'
        WHEN te.effectiveness_multiplier = 0.5 THEN 'Resistant (0.5x)'
        WHEN te.effectiveness_multiplier = 1 THEN 'Normal (1x)'
        WHEN te.effectiveness_multiplier = 2 THEN 'Super effective (2x)'
        WHEN te.effectiveness_multiplier = 4 THEN 'Doubly super effective (4x)'
        ELSE te.effectiveness_multiplier::TEXT || 'x'
    END AS effectiveness_category,
    
    -- Count Pokémon with each attacking and defending type
    COALESCE((SELECT SUM(pokemon_count) FROM pokemon_type_counts 
              WHERE primary_type = te.attacking_type OR secondary_type = te.attacking_type), 0) AS attacking_type_pokemon_count,
              
    COALESCE((SELECT SUM(pokemon_count) FROM pokemon_type_counts 
              WHERE primary_type = te.defending_type OR secondary_type = te.defending_type), 0) AS defending_type_pokemon_count,
    
    -- Dual-type effectiveness stats
    (
        SELECT COUNT(DISTINCT primary_defending_type || secondary_defending_type) 
        FROM dual_type_effectiveness dte 
        WHERE dte.attacking_type = te.attacking_type AND dte.combined_effectiveness = 0
    ) AS num_immune_type_combos,
    
    (
        SELECT COUNT(DISTINCT primary_defending_type || secondary_defending_type) 
        FROM dual_type_effectiveness dte 
        WHERE dte.attacking_type = te.attacking_type AND dte.combined_effectiveness = 0.25
    ) AS num_double_resistant_combos,
    
    (
        SELECT COUNT(DISTINCT primary_defending_type || secondary_defending_type) 
        FROM dual_type_effectiveness dte 
        WHERE dte.attacking_type = te.attacking_type AND dte.combined_effectiveness = 0.5
    ) AS num_resistant_combos,
    
    (
        SELECT COUNT(DISTINCT primary_defending_type || secondary_defending_type) 
        FROM dual_type_effectiveness dte 
        WHERE dte.attacking_type = te.attacking_type AND dte.combined_effectiveness = 2
    ) AS num_super_effective_combos,
    
    (
        SELECT COUNT(DISTINCT primary_defending_type || secondary_defending_type) 
        FROM dual_type_effectiveness dte 
        WHERE dte.attacking_type = te.attacking_type AND dte.combined_effectiveness = 4
    ) AS num_double_super_effective_combos,
    
    -- Most vulnerable dual-type to this attack
    (
        SELECT primary_defending_type || '/' || secondary_defending_type
        FROM dual_type_effectiveness dte
        WHERE dte.attacking_type = te.attacking_type
        ORDER BY dte.combined_effectiveness DESC, dte.pokemon_count DESC
        LIMIT 1
    ) AS most_vulnerable_dual_type,
    
    -- Most resistant dual-type to this attack
    (
        SELECT primary_defending_type || '/' || secondary_defending_type
        FROM dual_type_effectiveness dte
        WHERE dte.attacking_type = te.attacking_type AND dte.combined_effectiveness > 0
        ORDER BY dte.combined_effectiveness ASC, dte.pokemon_count DESC
        LIMIT 1
    ) AS most_resistant_dual_type,
    
    -- Enhanced matchup description with more detail
    CASE
        WHEN te.effectiveness_multiplier = 0 THEN 
            te.attacking_type || ' attacks have no effect on ' || te.defending_type || ' Pokémon'
        WHEN te.effectiveness_multiplier = 0.5 THEN 
            te.attacking_type || ' attacks are not very effective against ' || te.defending_type || ' Pokémon (0.5x damage)'
        WHEN te.effectiveness_multiplier = 1 THEN 
            te.attacking_type || ' attacks have normal effectiveness against ' || te.defending_type || ' Pokémon'
        WHEN te.effectiveness_multiplier = 2 THEN 
            te.attacking_type || ' attacks are super effective against ' || te.defending_type || ' Pokémon (2x damage)'
        WHEN te.effectiveness_multiplier = 4 THEN 
            te.attacking_type || ' attacks are doubly super effective against ' || te.defending_type || ' Pokémon (4x damage)'
        ELSE 
            te.attacking_type || ' vs ' || te.defending_type || ' = ' || te.effectiveness_multiplier || 'x damage'
    END AS matchup_description,
    
    -- Coverage value: How valuable is this attacking type for coverage?
    -- Higher = this type is super effective against more types with many Pokémon
    CASE
        WHEN te.effectiveness_multiplier >= 2 THEN 
            te.effectiveness_multiplier * COALESCE(tus.type_pokemon_count, 0)
        ELSE 0
    END AS coverage_value,
    
    -- Competitive relevance: How important is this matchup in competitive play?
    CASE
        WHEN te.defending_type IN ('Fairy', 'Dragon', 'Steel') AND te.effectiveness_multiplier >= 2 THEN 'Very High'
        WHEN te.defending_type IN ('Water', 'Flying', 'Ground', 'Ghost') AND te.effectiveness_multiplier >= 2 THEN 'High'
        WHEN te.effectiveness_multiplier = 4 THEN 'Very High'
        WHEN te.effectiveness_multiplier = 2 THEN 'Moderate'
        WHEN te.effectiveness_multiplier = 0 THEN 'Situational'
        ELSE 'Low'
    END AS competitive_relevance,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM type_effectiveness te
JOIN {{ ref('dim_pokemon_types') }} at ON te.attacking_type = at.type_name  -- Join for attacking type metadata
JOIN {{ ref('dim_pokemon_types') }} dt ON te.defending_type = dt.type_name  -- Join for defending type metadata
LEFT JOIN type_usage_stats tus ON te.defending_type = tus.defending_type  -- Add join for type usage stats
ORDER BY te.attacking_type, te.effectiveness_multiplier DESC