{{
  config(
    materialized = 'view',
    unique_key = 'id'
  )
}}

/*
  Model: stg_pokeapi_moves
  Description: Standardizes Pokémon move data from the PokeAPI
  
  Notes:
  - Fixed missing effect_text by extracting from JSON structure
  - Added safe type casting for numeric fields
  - Added metadata/derived fields for better analysis
  - Removed missing columns (created_at, updated_at)
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        type::JSONB AS type_json,
        power,
        pp,
        accuracy,
        priority,
        damage_class::JSONB AS damage_class_json,
        effect_entries::JSONB AS effect_entries,  -- Use effect_entries instead of effect_text
        effect_chance,
        generation::JSONB AS generation_json,     -- Renamed for clarity
        target::JSONB AS target_json
        -- Removed created_at and updated_at columns as they don't exist
    FROM {{ source('pokeapi', 'moves') }}
    WHERE id IS NOT NULL
),

-- Extract English effect description when available
effect_text_extract AS (
    SELECT
        id,
        (
            -- First try to find an English entry
            SELECT effect_entry->>'effect'
            FROM jsonb_array_elements(effect_entries) AS effect_entry
            WHERE effect_entry->'language'->>'name' = 'en'
            LIMIT 1
        ) AS effect_description_en,
        (
            -- Also get short effect
            SELECT effect_entry->>'short_effect'
            FROM jsonb_array_elements(effect_entries) AS effect_entry
            WHERE effect_entry->'language'->>'name' = 'en'
            LIMIT 1
        ) AS short_effect_en,
        -- Fallback to first entry if no English entry exists
        COALESCE(effect_entries->0->>'effect', '') AS effect_fallback
    FROM raw_data
)

SELECT
    -- Primary identifiers
    r.id,
    r.name AS move_name,
    
    -- Type information
    r.type_json->>'name' AS type_name,
    
    -- Move stats with safe numeric conversion
    CASE 
        WHEN r.power::TEXT ~ '^[0-9]+$' THEN r.power::INTEGER
        ELSE NULL 
    END AS power,
    
    CASE 
        WHEN r.pp::TEXT ~ '^[0-9]+$' THEN r.pp::INTEGER
        ELSE NULL 
    END AS pp,
    
    CASE 
        WHEN r.accuracy::TEXT ~ '^[0-9]+$' THEN r.accuracy::INTEGER
        ELSE NULL 
    END AS accuracy,
    
    CASE 
        WHEN r.priority::TEXT ~ '^[0-9]+$' THEN r.priority::INTEGER
        ELSE 0
    END AS priority,
    
    -- Effect information
    r.damage_class_json->>'name' AS damage_class,
    COALESCE(e.effect_description_en, e.effect_fallback) AS effect_text,
    e.short_effect_en AS short_effect_text,
    
    CASE 
        WHEN r.effect_chance::TEXT ~ '^[0-9]+$' THEN r.effect_chance::INTEGER
        ELSE NULL 
    END AS effect_chance,
    
    -- Target information
    r.target_json->>'name' AS target_type,
    
    -- Generation information
    r.generation_json->>'name' AS generation_name,
    CASE
        WHEN r.generation_json->>'name' ~ 'generation-([i|v]+)'
        THEN REGEXP_REPLACE(r.generation_json->>'name', 'generation-([i|v]+)', '\1')
        ELSE NULL
    END AS generation_number,
    
    -- Derived fields for analysis
    CASE
        WHEN r.damage_class_json->>'name' = 'physical' THEN 'Physical'
        WHEN r.damage_class_json->>'name' = 'special' THEN 'Special'
        ELSE 'Status'
    END AS move_category,
    
    CASE
        -- Power tiers for damaging moves
        WHEN r.damage_class_json->>'name' IN ('physical', 'special') THEN
            CASE 
                WHEN r.power::TEXT ~ '^[0-9]+$' THEN
                    CASE
                        WHEN r.power::INTEGER >= 120 THEN 'Ultra High'
                        WHEN r.power::INTEGER >= 90 THEN 'Very High'
                        WHEN r.power::INTEGER >= 70 THEN 'High'
                        WHEN r.power::INTEGER >= 50 THEN 'Medium'
                        WHEN r.power::INTEGER > 0 THEN 'Low'
                        ELSE 'No Power'
                    END
                ELSE 'Unknown'
            END
        ELSE 'Status Move'
    END AS power_tier,
    
    -- Source tracking - removed missing source fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data r
LEFT JOIN effect_text_extract e ON r.id = e.id