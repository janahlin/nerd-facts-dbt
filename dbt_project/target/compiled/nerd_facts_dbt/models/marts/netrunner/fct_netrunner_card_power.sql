/*
  Model: fct_netrunner_card_power (Simplified)
  Description: Basic version of card power metrics
*/

WITH card_base AS (
    SELECT
        code AS card_code,
        id AS card_id,
        card_name,  
        type_name,  
        side_code,
        faction_code,
        faction_name,
        pack_code
    FROM "nerd_facts"."public"."stg_netrunner_cards"
    WHERE code IS NOT NULL
)

SELECT
    -- Generate surrogate key for fact table
    md5(cast(coalesce(cast(c.card_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_power_key,
    
    -- Foreign keys
    md5(cast(coalesce(cast(c.card_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_key,
    md5(cast(coalesce(cast(c.faction_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS faction_key,
    md5(cast(coalesce(cast(c.pack_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pack_key,
    md5(cast(coalesce(cast(c.side_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(c.type_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS card_type_key,
    
    -- Card identifiers
    c.card_code,
    c.card_id,
    c.card_name,
    c.type_name,
    c.faction_code,
    c.faction_name,
    c.side_code,
    c.pack_code,
    
    -- Card timing classification (simple)
    CASE
        WHEN c.type_name IN ('Event', 'Operation') THEN 'One-time'
        WHEN c.type_name IN ('Asset', 'Resource', 'Hardware', 'Program') THEN 'Permanent'
        WHEN c.type_name = 'ICE' THEN 'Defensive'
        WHEN c.type_name = 'Agenda' THEN 'Objective'
        ELSE 'Other'
    END AS card_timing,
    
    -- Simple power level classification (no calculations)
    CASE
        WHEN c.card_name IN ('Account Siphon', 'Corroder', 'Medium', 'Parasite', 
                           'SanSan City Grid', 'Astroscript Pilot Program', 'Desperado',
                           'Sure Gamble', 'Hedge Fund', 'Jackson Howard') THEN 'High'
        WHEN c.type_name IN ('Identity', 'Console') THEN 'High'
        ELSE 'Standard'
    END AS power_level,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM card_base c
ORDER BY c.side_code, c.faction_code, c.card_name