/*
  Model: fct_netrunner_card_power
  Description: Fact table for Android: Netrunner card power metrics and evaluations
  
  Notes:
  - Provides detailed card power assessments using multiple dimensions
  - Calculates efficiency scores for different card types
  - Evaluates meta relevance and rotation status
  - Analyzes text patterns for synergy detection
  - Differs from fact_netrunner_cards by focusing on power/efficiency metrics rather than general card facts
*/

WITH card_base AS (
    SELECT
        c.code AS card_code,
        c.id AS card_id,
        c.card_name,  -- Updated from 'title' based on staging model
        c.type_name,  -- Updated from 'card_type' based on staging model
        c.side_code,
        c.side_name,
        c.faction_code,
        c.faction_name, -- Already included from improved stg_netrunner_cards
        c.influence_cost, -- Updated from 'influence'
        c.cost,
        c.strength,
        c.advancement_requirement,
        c.agenda_points,
        c.card_text,  -- Updated from 'text'
        c.pack_code,  -- Added missing pack_code
        c.memory_cost,
        c.trash_cost,
        c.is_unique,
        c.keywords_array
    FROM {{ ref('stg_netrunner_cards') }} c  -- Updated from stg_netrunner
    WHERE c.id IS NOT NULL
),

-- Add pack release information
card_release AS (
    SELECT
        cb.*,
        p.release_date,
        p.cycle_code,
        p.cycle_name,
        -- Calculate how many days ago this card was released (with proper null handling)
        CASE
            WHEN p.release_date IS NULL THEN NULL
            ELSE EXTRACT(DAYS FROM CURRENT_DATE - p.release_date) 
        END AS days_since_release
    FROM card_base cb
    LEFT JOIN {{ ref('stg_netrunner_packs') }} p ON cb.pack_code = p.code
)

SELECT
    -- Generate surrogate key for fact table
    {{ dbt_utils.generate_surrogate_key(['cr.card_code']) }} AS card_power_key,
    
    -- Foreign keys
    {{ dbt_utils.generate_surrogate_key(['cr.card_code']) }} AS card_key,
    {{ dbt_utils.generate_surrogate_key(['cr.faction_code']) }} AS faction_key,
    {{ dbt_utils.generate_surrogate_key(['cr.pack_code']) }} AS pack_key,
    {{ dbt_utils.generate_surrogate_key(['cr.side_code', 'cr.type_name']) }} AS card_type_key,
    
    -- Card identifiers
    cr.card_code,
    cr.card_id,
    cr.card_name,
    cr.type_name,
    cr.faction_code,
    cr.faction_name,
    cr.side_code,
    cr.side_name,
    cr.pack_code,
    cr.cycle_code,
    
    -- Core card metrics with NULL handling
    COALESCE(cr.influence_cost, 0) AS influence_cost,
    COALESCE(cr.cost, 0) AS cost,
    COALESCE(cr.strength, 0) AS strength,
    COALESCE(cr.advancement_requirement, 0) AS advancement_requirement,
    COALESCE(cr.agenda_points, 0) AS agenda_points,
    COALESCE(cr.memory_cost, 0) AS memory_cost,
    COALESCE(cr.trash_cost, 0) AS trash_cost,
    
    -- Card economy efficiency with improved regex and error handling
    CASE
        -- Operations and Events (one-time economy)
        WHEN cr.type_name IN ('Operation', 'Event') AND cr.card_text ILIKE '%gain%credit%' THEN
            COALESCE(
                NULLIF(REGEXP_REPLACE(
                    REGEXP_REPLACE(cr.card_text, '.*gain ([0-9]+)[^0-9].*', '\1', 'g'),
                    '[^0-9]', '', 'g'
                ), '')::INTEGER, 0
            ) - COALESCE(cr.cost, 0)
        -- Assets and Resources (recurring economy)
        WHEN cr.type_name IN ('Asset', 'Resource') AND cr.card_text ILIKE '%gain%credit%' THEN
            GREATEST(0, (
                COALESCE(
                    NULLIF(REGEXP_REPLACE(
                        REGEXP_REPLACE(cr.card_text, '.*gain ([0-9]+)[^0-9].*', '\1', 'g'),
                        '[^0-9]', '', 'g'
                    ), '')::INTEGER, 0
                ) - COALESCE(cr.cost, 0)
            ) / 2)
        -- Fixed economy cards
        WHEN cr.card_name IN ('Hedge Fund', 'Sure Gamble', 'IPO') THEN 4
        WHEN cr.card_name IN ('Daily Casts', 'Adonis Campaign') THEN 3
        WHEN cr.card_name IN ('PAD Campaign', 'Dirty Laundry') THEN 2
        ELSE 0
    END AS credit_efficiency,
    
    -- Card power metrics with improved type handling
    CASE
        -- Agenda efficiency
        WHEN cr.type_name = 'Agenda' THEN 
            CASE
                WHEN COALESCE(cr.agenda_points, 0) > 0 AND COALESCE(cr.advancement_requirement, 0) > 0 
                THEN ROUND((cr.agenda_points::FLOAT / NULLIF(cr.advancement_requirement, 0)) * 5, 1)
                ELSE 3
            END
        -- ICE efficiency
        WHEN cr.type_name = 'ICE' THEN
            CASE 
                WHEN cr.strength IS NULL THEN 2
                WHEN COALESCE(cr.cost, 0) > 0 THEN ROUND((cr.strength::FLOAT / NULLIF(cr.cost, 0)) * 2, 1)
                ELSE COALESCE(cr.strength, 2)
            END
        -- Icebreaker efficiency
        WHEN cr.type_name = 'Program' AND cr.card_text ILIKE '%Icebreaker%' THEN
            CASE
                WHEN cr.strength IS NULL THEN 2
                WHEN COALESCE(cr.cost, 0) > 0 THEN ROUND((cr.strength::FLOAT / NULLIF(cr.cost, 0)) * 2, 1)
                ELSE COALESCE(cr.strength, 2)
            END
        -- Identity and special card types
        WHEN cr.type_name = 'Identity' THEN 4
        -- Default cost-based efficiency
        WHEN cr.cost IS NULL THEN 3
        ELSE GREATEST(1, 5 - COALESCE(cr.cost::FLOAT, 0) / 2)
    END AS efficiency_score,
    
    -- Card power level classification - improved with more indicators
    CASE
        -- Known powerful/iconic cards
        WHEN cr.card_name IN ('Account Siphon', 'Corroder', 'Medium', 'Parasite', 
                           'SanSan City Grid', 'Astroscript Pilot Program', 'Desperado',
                           'Sure Gamble', 'Hedge Fund', 'Jackson Howard', 
                           'Scorched Earth', 'Daily Business Show', 'Breaking News') THEN 5
                           
        -- Powerful card types with good stats
        WHEN cr.type_name = 'Agenda' AND 
             COALESCE(cr.agenda_points, 0) >= 3 AND 
             COALESCE(cr.advancement_requirement, 0) <= 5 THEN 5
             
        WHEN cr.type_name = 'ICE' AND 
             COALESCE(cr.strength, 0) >= 4 AND 
             COALESCE(cr.cost, 0) <= 4 THEN 4
             
        WHEN cr.type_name IN ('Console', 'Identity') THEN 4
        
        -- Unique and high-influence cards
        WHEN cr.is_unique AND COALESCE(cr.influence_cost, 0) >= 3 THEN 4
        WHEN COALESCE(cr.influence_cost, 0) >= 4 THEN 4
        WHEN COALESCE(cr.influence_cost, 0) >= 2 THEN 3
        
        -- Consider keyword count - more keywords often mean more powerful cards
        WHEN ARRAY_LENGTH(cr.keywords_array, 1) >= 3 THEN 4
        
        -- Default
        ELSE 2
    END AS power_level,
    
    -- Meta relevance with improved calculation
    CASE
        -- New releases
        WHEN cr.days_since_release < 90 THEN 'New Release'
        
        -- Known meta-defining cards
        WHEN cr.card_name IN ('Account Siphon', 'Corroder', 'Sure Gamble', 'Hedge Fund',
                           'Astroscript Pilot Program', 'SanSan City Grid', 
                           'Jackson Howard', 'Desperado', 'Medium') THEN 'Meta Defining'
                           
        -- High power level cards
        WHEN (
            cr.card_name IN ('Breaking News', 'Scorched Earth', 'Daily Business Show',
                         'Parasite', 'Eli 1.0', 'Caprice Nisei', 'Clone Chip')
            OR 
            (cr.type_name = 'Agenda' AND 
             COALESCE(cr.agenda_points, 0) >= 3 AND 
             COALESCE(cr.advancement_requirement, 0) <= 5)
            OR
            (cr.is_unique AND COALESCE(cr.influence_cost, 0) >= 3)
        ) THEN 'Meta Relevant'
        
        -- Standard cards
        ELSE 'Standard'
    END AS meta_status,
    
    -- Card synergy indicators with improved text pattern matching
    CASE
        -- Anarch synergies
        WHEN (cr.card_text ILIKE '%virus%' OR 
              cr.card_text ILIKE '%trash%program%' OR
              cr.card_text ILIKE '%install%virus%') 
             AND cr.faction_code = 'anarch' THEN TRUE
             
        -- Shaper synergies
        WHEN (cr.card_text ILIKE '%stealth%' OR 
              cr.card_text ILIKE '%memory%' OR
              cr.card_text ILIKE '%install%program%') 
             AND cr.faction_code = 'shaper' THEN TRUE
             
        -- Criminal synergies  
        WHEN (cr.card_text ILIKE '%bypass%' OR 
              cr.card_text ILIKE '%credit%successful run%' OR
              cr.card_text ILIKE '%expose%') 
             AND cr.faction_code = 'criminal' THEN TRUE
             
        -- NBN synergies
        WHEN (cr.card_text ILIKE '%tag%' OR 
              cr.card_text ILIKE '%trace%' OR
              cr.card_text ILIKE '%reveal%') 
             AND cr.faction_code = 'nbn' THEN TRUE
             
        -- Jinteki synergies
        WHEN (cr.card_text ILIKE '%advance%' OR 
              cr.card_text ILIKE '%net damage%' OR
              cr.card_text ILIKE '%reveal%') 
             AND cr.faction_code = 'jinteki' THEN TRUE
             
        -- Weyland synergies
        WHEN (cr.card_text ILIKE '%bad publicity%' OR 
              cr.card_text ILIKE '%meat damage%' OR
              cr.card_text ILIKE '%trash%resource%') 
             AND cr.faction_code = 'weyland-consortium' THEN TRUE
             
        -- HB synergies
        WHEN (cr.card_text ILIKE '%bioroid%' OR 
              cr.card_text ILIKE '%brain damage%' OR
              cr.card_text ILIKE '%click%break%') 
             AND cr.faction_code = 'haas-bioroid' THEN TRUE
             
        -- Default
        ELSE FALSE
    END AS has_faction_synergy,
    
    -- Post-rotation status with up-to-date cycle information
    CASE
        WHEN cr.cycle_code IN ('core', 'genesis', 'creation-and-control', 'spin', 
                            'honor-and-profit', 'lunar', 'order-and-chaos', 
                            'sansan', 'mumbad', 'flashpoint', 'red-sand') THEN 'Rotated'
        WHEN cr.cycle_code IN ('terminal-directive', 'core2', 'kitara', 'reign-and-reverie') THEN 'Standard'
        WHEN cr.cycle_code IS NULL THEN 'Unknown'
        ELSE 'Standard'
    END AS rotation_status,
    
    -- Calculate overall card rating (composite score)
    CASE
        -- Meta-defining cards
        WHEN cr.card_name IN ('Account Siphon', 'Astroscript Pilot Program', 'Jackson Howard') THEN 95
        -- Rating based on multiple factors
        ELSE GREATEST(0, LEAST(100, (
            (COALESCE(cr.influence_cost, 0) * 5) +
            CASE WHEN cr.is_unique THEN 10 ELSE 0 END +
            CASE 
                WHEN cr.type_name = 'Agenda' THEN (COALESCE(cr.agenda_points, 0) * 20) - (COALESCE(cr.advancement_requirement, 0) * 5)
                WHEN cr.type_name = 'ICE' THEN (COALESCE(cr.strength, 0) * 5) 
                WHEN cr.type_name = 'Program' AND cr.card_text ILIKE '%Icebreaker%' THEN (COALESCE(cr.strength, 0) * 7)
                WHEN cr.type_name IN ('Identity', 'Console') THEN 70
                ELSE 50
            END +
            CASE WHEN cr.card_name IN ('Sure Gamble', 'Hedge Fund') THEN 30 ELSE 0 END +
            CASE WHEN cr.card_text ILIKE '%gain%credit%' THEN 15 ELSE 0 END
        )))
    END AS card_rating,
    
    -- Card timing classification
    CASE
        WHEN cr.type_name IN ('Event', 'Operation') THEN 'One-time'
        WHEN cr.type_name IN ('Asset', 'Resource', 'Hardware', 'Program', 'Identity', 'Console') THEN 'Permanent'
        WHEN cr.type_name = 'ICE' THEN 'Defensive'
        WHEN cr.type_name = 'Upgrade' THEN 'Enhancement'
        WHEN cr.type_name = 'Agenda' THEN 'Objective'
        ELSE 'Other'
    END AS card_timing,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM card_release cr
ORDER BY cr.side_name, cr.faction_name, power_level DESC, cr.card_name