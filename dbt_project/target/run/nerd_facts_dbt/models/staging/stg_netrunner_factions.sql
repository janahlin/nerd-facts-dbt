
  create view "nerd_facts"."public"."stg_netrunner_factions__dbt_tmp"
    
    
  as (
    WITH raw_data AS (
    SELECT DISTINCT code, name
    FROM raw.netrunner_factions
)
SELECT
    code AS faction_code,
    name AS faction_name
FROM raw_data
  );