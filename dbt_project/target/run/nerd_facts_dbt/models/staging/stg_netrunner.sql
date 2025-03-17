
  create view "nerd_facts"."public"."stg_netrunner__dbt_tmp"
    
    
  as (
    WITH raw_data AS (
    SELECT * FROM raw.netrunner_cards
)
SELECT
    c.id,
    c.title AS card_name,
    c.faction_code,
    f.name AS faction_name,  
    c.type_code,
    t.name AS type_name,  
    CASE 
        WHEN c.cost IN ('NaN', '') THEN NULL  -- ✅ Replace NaN or empty strings with NULL
        ELSE c.cost::NUMERIC::INTEGER 
    END AS cost,  -- ✅ Convert only valid numbers
    c.text AS description
FROM raw_data c
LEFT JOIN raw.netrunner_factions f ON c.faction_code = f.code
LEFT JOIN raw.netrunner_types t ON c.type_code = t.code
  );