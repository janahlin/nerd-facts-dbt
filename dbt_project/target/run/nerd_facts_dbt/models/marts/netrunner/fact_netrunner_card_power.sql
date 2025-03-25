
  
    

  create  table "nerd_facts"."public"."fact_netrunner_card_power__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: fact_netrunner_card_power (Ultra-Simplified)
  Description: Card power metrics from the Netrunner universe
*/

SELECT
    md5(cast(coalesce(cast(c.code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_key,
    c.code AS card_id,
    c.card_name,
    c.faction_code,
    c.type_code,
    c.cost,
    
    -- Simplified scoring metrics
    CASE 
        WHEN c.type_code = 'agenda' THEN COALESCE(c.advancement_cost, 0) * 2
        WHEN c.type_code IN ('ice', 'program') THEN COALESCE(c.strength, 0) * 3
        ELSE COALESCE(c.cost, 0) * 2
    END AS card_power_score,
    
    -- Relative power tier
    CASE 
        WHEN c.type_code = 'agenda' AND COALESCE(c.advancement_cost, 0) >= 5 THEN 'High'
        WHEN c.type_code IN ('ice', 'program') AND COALESCE(c.strength, 0) >= 4 THEN 'High'
        WHEN COALESCE(c.cost, 0) >= 4 THEN 'High'
        WHEN c.type_code = 'agenda' AND COALESCE(c.advancement_cost, 0) >= 3 THEN 'Medium'
        WHEN c.type_code IN ('ice', 'program') AND COALESCE(c.strength, 0) >= 2 THEN 'Medium'
        WHEN COALESCE(c.cost, 0) >= 2 THEN 'Medium'
        ELSE 'Low'
    END AS power_tier,
    
    'netrunner' AS universe,
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM "nerd_facts"."public"."stg_netrunner_cards" c
WHERE c.code IS NOT NULL
ORDER BY card_power_score DESC
  );
  