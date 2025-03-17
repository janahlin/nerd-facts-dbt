/*
  Model: stg_netrunner_factions
  Description: Standardizes Netrunner faction data from the raw source
  Source: raw.netrunner_factions
  
  Note: DISTINCT is used to prevent duplicate faction entries
  that might exist in the data source due to multiple versions.
*/

WITH raw_data AS (
    -- Select all relevant columns from source
    SELECT DISTINCT
        code,
        name,
        side_code,         -- Corporation or Runner side
        is_mini,           -- Whether this is a mini-faction
        color,             -- Faction color for UI visualization
        description        -- Faction description text
    FROM raw.netrunner_factions
    WHERE code IS NOT NULL -- Ensure we don't include invalid entries
)

SELECT
    -- Primary identifiers
    code AS faction_code,
    name AS faction_name,
    
    -- Side information
    side_code,
    CASE 
        WHEN side_code = 'corp' THEN 'Corporation'
        WHEN side_code = 'runner' THEN 'Runner'
        ELSE 'Unknown'
    END AS side_name,
    
    -- Faction attributes
    COALESCE(is_mini, FALSE) AS is_mini, -- Default to FALSE if NULL
    color,
    description AS faction_description,
    
    -- Additional derived attributes
    CASE
        WHEN code IN ('haas-bioroid', 'jinteki', 'nbn', 'weyland-consortium') THEN TRUE
        WHEN code IN ('anarch', 'criminal', 'shaper') THEN TRUE
        ELSE FALSE
    END AS is_core_faction,
    
    -- Track record creation
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM raw_data
