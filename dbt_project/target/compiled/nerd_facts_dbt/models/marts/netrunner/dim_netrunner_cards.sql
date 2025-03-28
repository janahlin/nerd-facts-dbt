/*
  Model: dim_netrunner_cards
  Description: Bare minimum dimension table for Android: Netrunner cards
  
  Note: No type casting to avoid numeric conversion issues
*/

SELECT
    -- Primary identifiers
    code AS card_code,
    card_id,
    card_name,
    
    -- Card classifications  
    type_code,
    faction_code,
    side_code,
    
    -- Raw values without any casting
    faction_cost AS influence_cost,
    cost,
    
    -- Boolean value
    uniqueness AS is_unique_card,
    
    -- Pack information
    pack_code,
    
    -- Add tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM "nerd_facts"."public"."stg_netrunner_cards"
WHERE code IS NOT NULL
ORDER BY side_code, faction_code, type_code