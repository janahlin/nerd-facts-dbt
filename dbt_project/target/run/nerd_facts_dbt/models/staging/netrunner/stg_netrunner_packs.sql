
  create view "nerd_facts"."public"."stg_netrunner_packs__dbt_tmp"
    
    
  as (
    /*
  Model: stg_netrunner_packs
  Description: Standardizes Netrunner card pack data from the raw source
  Source: raw.netrunner_packs
  
  Packs represent the physical card sets released for Netrunner, such as core sets,
  deluxe expansions, and data packs within a cycle.
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        code,
        name,
        cycle_code,

        -- Numeric fields
        CASE WHEN(position~E'^[0-9]+$') THEN position ELSE NULL END AS position,
        CASE WHEN(size~E'^[0-9]+$') THEN size ELSE NULL END AS size,
        CASE WHEN ffg_id~E'^[0-9]+$' THEN ffg_id ELSE NULL END AS ffg_id,

        -- Date fields
        date_release
                
    FROM "nerd_facts"."raw"."netrunner_packs"  -- Updated to use source macro
    WHERE code IS NOT NULL -- Filter out invalid entries
)

SELECT
    -- Primary identifiers
    id AS pack_id,

    -- Text fields
    code,
    name as pack_name,
    cycle_code,

    -- Numeric fields
    CAST(position AS NUMERIC) AS position,
    CAST(size AS NUMERIC) AS size,
    CAST(ffg_id AS NUMERIC) AS ffg_id, 

    -- Date fields
    date_release AS release_at,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
  );