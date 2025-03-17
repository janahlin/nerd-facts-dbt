/*
  Model: dim_netrunner_cards
  Description: Bare minimum dimension table for Android: Netrunner cards
  
  Note: No type casting to avoid numeric conversion issues
*/

SELECT
    -- Primary identifiers
    code AS card_code,
    id AS card_id,
    card_name,
    
    -- Card classifications  
    type_name,
    faction_name,
    side_code,
    
    -- Raw values without any casting
    influence_cost,
    cost,
    
    -- Boolean value
    is_unique_card,
    
    -- Pack information
    pack_code,
    
    -- Add tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM "nerd_facts"."public"."stg_netrunner_cards"
WHERE code IS NOT NULL
ORDER BY side_code, faction_name, type_name