
  
    

  create  table "nerd_facts"."public"."fact_netrunner_cards__dbt_tmp"
  
  
    as
  
  (
    /*
  Model: fact_netrunner_cards
  Description: Fact table for Android: Netrunner cards with metrics and relationships
  
  Notes:
  - Contains key metrics about card usage, efficiency, and value
  - Links to dimension tables for cards, factions, packs, and types
  - Includes derived metrics for cards such as cost efficiency and power indices
  - Provides temporal context through release dates and rotation status
  - Combines multiple data sources for comprehensive analysis
*/

WITH card_metrics AS (
    -- Calculate and derive card-specific metrics
    SELECT
        c.code AS card_code,
        c.card_id,
        c.card_name,
        c.faction_code,
        c.side_code,
        c.type_code,
        c.pack_code,
        
        -- Core card attributes - keep as text for now
        c.cost,
        c.strength,
        c.advancement_cost,
        c.agenda_points,
        c.memory_cost,
        c.trash_cost,
        c.faction_cost,
        
        -- Calculate cost efficiency metrics with proper type casting
        CASE 
            -- For ICE, calculate strength-to-cost ratio
            WHEN c.type_code = 'ice' AND c.cost::TEXT ~ '^[0-9]+$' AND c.cost::NUMERIC > 0 
                 AND c.strength IS NOT NULL AND c.strength::TEXT ~ '^[0-9]+(\.[0-9]+)?$'
            THEN ROUND((c.strength::NUMERIC / c.cost::NUMERIC), 2)
            
            -- For Agendas, calculate points-to-advancement ratio
            WHEN c.type_code = 'agenda' AND c.advancement_cost::TEXT ~ '^[0-9]+$' 
                 AND c.advancement_cost::NUMERIC > 0 AND c.agenda_points IS NOT NULL 
                 AND c.agenda_points::TEXT ~ '^[0-9]+$'
            THEN ROUND((c.agenda_points::NUMERIC / c.advancement_cost::NUMERIC), 2)
            
            -- For Programs/Hardware, basic cost efficiency if applicable
            WHEN c.type_code IN ('program', 'hardware') AND c.cost::TEXT ~ '^[0-9]+$' 
                 AND c.cost::NUMERIC > 0 AND c.strength IS NOT NULL 
                 AND c.strength::TEXT ~ '^[0-9]+(\.[0-9]+)?$'
            THEN ROUND((c.strength::NUMERIC / c.cost::NUMERIC), 2)
            
            ELSE NULL
        END AS cost_efficiency_ratio,
        
        -- Calculate text length (proxy for card complexity)
        LENGTH(c.text) AS text_length,
        
        -- Calculate influence efficiency with safe type casting
        CASE
            WHEN c.faction_cost::TEXT ~ '^[0-9]+$' AND c.faction_cost::NUMERIC > 0 
                 AND c.agenda_points IS NOT NULL AND c.agenda_points::TEXT ~ '^[0-9]+$'
            THEN ROUND((c.agenda_points::NUMERIC / c.faction_cost::NUMERIC), 2)
            
            WHEN c.faction_cost::TEXT ~ '^[0-9]+$' AND c.faction_cost::NUMERIC > 0 
                 AND c.strength IS NOT NULL AND c.strength::TEXT ~ '^[0-9]+(\.[0-9]+)?$'
            THEN ROUND((c.strength::NUMERIC / c.faction_cost::NUMERIC), 2)
            
            ELSE NULL
        END AS influence_efficiency,
        
        -- Keywords count
        CASE WHEN c.keywords IS NOT NULL 
             THEN COALESCE(ARRAY_LENGTH(STRING_TO_ARRAY(c.keywords, ' - '), 1), 0)
             ELSE 0 
        END AS keyword_count,
        
        -- Reference pack info
        p.release_at,
        p.cycle_code,
        
        -- Simulated usage data - no type casting needed here
        CASE
            WHEN c.card_name IN ('Account Siphon', 'Desperado', 'SanSan City Grid', 
                              'Astroscript Pilot Program', 'Hedge Fund', 'Sure Gamble') THEN 95
            WHEN c.pack_code = 'core' AND c.faction_code IS NOT NULL THEN 70
            WHEN c.faction_code IS NULL THEN 30
            WHEN p.cycle_code IN ('genesis', 'creation-and-control') THEN 60
            WHEN p.release_at IS NULL THEN 20
            WHEN DATE_PART('year', p.release_at::timestamp) <= 2014 THEN 65
            WHEN DATE_PART('year', p.release_at::timestamp) <= 2016 THEN 50
            ELSE 40
        END AS popularity_score
        
    FROM "nerd_facts"."public"."stg_netrunner_cards" c
    LEFT JOIN "nerd_facts"."public"."stg_netrunner_packs" p ON c.pack_code = p.code
)

SELECT
    -- Primary keys and relationships
    md5(cast(coalesce(cast(cm.card_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_fact_key,
    cm.card_code,
    cm.card_id,
    
    -- Foreign keys to dimension tables
    md5(cast(coalesce(cast(cm.card_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_key,
    md5(cast(coalesce(cast(cm.faction_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS faction_key,
    md5(cast(coalesce(cast(cm.pack_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pack_key,
    md5(cast(coalesce(cast(cm.side_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(cm.type_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_type_key,
    
    -- Essential card attributes 
    cm.card_name,
    cm.faction_code,
    cm.side_code,
    cm.type_code,
    cm.pack_code,
    
    -- Core card metrics with safe type handling
    CASE WHEN cm.cost::TEXT ~ '^[0-9]+$' THEN cm.cost::INTEGER ELSE 0 END AS cost,
    CASE WHEN cm.strength::TEXT ~ '^[0-9\.]+$' THEN cm.strength::NUMERIC ELSE 0 END AS strength,
    CASE WHEN cm.advancement_cost::TEXT ~ '^[0-9]+$' THEN cm.advancement_cost::INTEGER ELSE 0 END AS advancement_requirement,
    CASE WHEN cm.agenda_points::TEXT ~ '^[0-9]+$' THEN cm.agenda_points::INTEGER ELSE 0 END AS agenda_points,
    CASE WHEN cm.memory_cost::TEXT ~ '^[0-9]+$' THEN cm.memory_cost::INTEGER ELSE 0 END AS memory_cost,
    CASE WHEN cm.trash_cost::TEXT ~ '^[0-9]+$' THEN cm.trash_cost::INTEGER ELSE 0 END AS trash_cost,
    
    -- Calculated metrics
    cm.cost_efficiency_ratio,
    cm.text_length AS complexity_score,
    cm.influence_efficiency,
    cm.keyword_count,
    
    -- Usage and popularity metrics
    cm.popularity_score,
    CASE
        WHEN cm.popularity_score >= 80 THEN 'Meta Defining'
        WHEN cm.popularity_score >= 60 THEN 'Staple'
        WHEN cm.popularity_score >= 40 THEN 'Playable'
        ELSE 'Niche'
    END AS popularity_tier,
    
    -- Deck construction significance
    CASE
        WHEN cm.card_name IN ('Hedge Fund', 'Sure Gamble', 'IPO', 'Dirty Laundry') THEN 'Auto-include'
        WHEN cm.popularity_score >= 75 THEN 'High Impact'
        WHEN cm.popularity_score >= 50 THEN 'Medium Impact'
        ELSE 'Low Impact'
    END AS deck_impact,
    
    -- Time dimensions
    cm.release_at,
    cm.cycle_code,
    CASE 
        WHEN cm.cycle_code IN ('core', 'genesis', 'creation-and-control', 'spin') THEN 'First Rotation'
        WHEN cm.cycle_code IN ('lunar', 'order-and-chaos', 'sansan', 'mumbad') THEN 'Second Rotation'
        ELSE 'Current'
    END AS rotation_group,
    
    -- Card pool status (current as of 2025)
    CASE
        WHEN cm.cycle_code IN ('core', 'genesis', 'creation-and-control', 'spin', 
                           'lunar', 'order-and-chaos', 'sansan', 'mumbad',
                           'flashpoint', 'red-sand') THEN 'Rotated'
        WHEN cm.cycle_code IN ('terminal-directive', 'core2', 'kitara', 'reign-and-reverie') THEN 'Standard'
        WHEN cm.cycle_code IS NULL THEN 'Unknown'
        ELSE 'Standard'
    END AS card_pool_status,
    
    -- Card value index (composite score)
    ROUND(
        (COALESCE(cm.popularity_score, 0) * 0.6) + 
        (COALESCE(cm.cost_efficiency_ratio, 0) * 20) +
        (CASE WHEN cm.type_code = 'identity' THEN 30 ELSE 0 END) +
        (CASE WHEN cm.card_name IN ('Account Siphon', 'Astroscript Pilot Program', 'Jackson Howard') THEN 40 ELSE 0 END)
    ) AS card_value_index,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM card_metrics cm
  );
  