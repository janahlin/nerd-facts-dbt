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
        id,
        code,
        name,
        position,
        date_release,
        size,
        cycle_code
    FROM {{ source('netrunner', 'packs') }}  -- Updated to use source macro
    WHERE code IS NOT NULL -- Filter out invalid entries
)

SELECT
    -- Primary identifiers
    id,
    code,
    name AS pack_name,
    
    -- Release information
    CASE
        WHEN date_release = '' THEN NULL
        ELSE TO_DATE(date_release, 'YYYY-MM-DD')
    END AS release_date,
    
    -- Pack attributes
    position AS pack_position,
    NULLIF(size::TEXT, '0')::INTEGER AS card_count,  -- Fixed type casting
    cycle_code,
    
    -- Derived fields
    CASE
        WHEN name ILIKE '%core%' THEN 'Core'
        WHEN name ILIKE '%deluxe%' THEN 'Deluxe'
        WHEN name ILIKE '%draft%' THEN 'Draft'
        ELSE 'Data Pack'
    END AS pack_type,
    
    -- Calculate approximate rotation status
    CASE
        WHEN cycle_code IN ('core', 'genesis', 'creation-and-control', 'spin', 'honor-and-profit', 'lunar') 
        THEN 'Rotated'
        WHEN cycle_code IS NULL THEN 'Unknown'
        ELSE 'Legal'
    END AS rotation_status,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
