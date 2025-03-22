
  create view "nerd_facts"."public"."stg_netrunner_factions__dbt_tmp"
    
    
  as (
    /*
  Model: stg_netrunner_factions
  Description: Standardizes Netrunner faction data from the raw source
  Source: raw.netrunner_factions
  
*/

WITH raw_data AS (
    -- Select all relevant columns from source
    SELECT
        -- Primary identifiers
        id, 

        -- Text fields
        code,
        name,
        side_code,
        is_mini,
        is_neutral,
        color 
    FROM "nerd_facts"."raw"."netrunner_factions"
    WHERE code IS NOT NULL -- Ensure we don't include invalid entries
)

SELECT
         -- Primary identifiers
        id as faction_id, 

        -- Text fields
        code,
        name as faction_name,
        side_code,
        is_mini,
        is_neutral,
        color,
    
    -- Track record creation
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM raw_data
  );