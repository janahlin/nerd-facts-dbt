
  
    

  create  table "nerd_facts"."public"."dim_netrunner_factions__dbt_tmp"
  
  
    as
  
  (
    WITH factions AS (
    SELECT DISTINCT ON (f.faction_name)
        f.faction_name,
        COUNT(*) OVER (PARTITION BY f.faction_name) AS num_cards
    FROM "nerd_facts"."public"."stg_netrunner" n
    LEFT JOIN "nerd_facts"."public"."stg_netrunner_factions" f ON n.faction_code = f.faction_code
)
SELECT * FROM factions
  );
  