/*
  Model: stg_pokeapi_items
  Description: Standardizes Pokémon item data from the PokeAPI
  Source: raw.pokeapi_items
  
  Notes:
  - Effect entries are extracted with language preference for English entries
  - Categories and attributes are parsed from nested JSON
  - Item classifications are derived from item attributes and names
  - Fixed missing sprite_url column issue
  - Fixed type casting issues with numeric fields
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        cost,
        effect_entries::JSONB AS effect_entries, -- Added explicit casting
        fling_power,
        fling_effect::JSONB AS fling_effect,     -- Added explicit casting
        attributes::JSONB AS attributes,         -- Added explicit casting
        category::JSONB AS category              -- Added explicit casting
        -- Removed sprite_url as it doesn't exist in source
    FROM raw.pokeapi_items
    WHERE id IS NOT NULL
),

-- Extract English effect description when available
effect_text AS (
    SELECT
        id,
        (
            -- First try to find an English entry
            SELECT effect_entry->>'effect'
            FROM jsonb_array_elements(effect_entries) AS effect_entry
            WHERE effect_entry->'language'->>'name' = 'en'
            LIMIT 1
        ) AS effect_en,
        -- Fallback to first entry if no English entry
        COALESCE(effect_entries->0->>'effect', '') AS effect_fallback
    FROM raw_data
)

SELECT
    -- Primary identifiers
    r.id,
    r.name AS item_name,
    
    -- Item attributes with proper casting
    CASE 
        WHEN r.cost::TEXT ~ '^[0-9]+$' THEN 
            NULLIF(r.cost::INTEGER, 0) 
        ELSE NULL 
    END AS purchase_cost,
    
    CASE 
        WHEN r.fling_power::TEXT ~ '^[0-9]+$' THEN 
            NULLIF(r.fling_power::INTEGER, 0) 
        ELSE NULL 
    END AS fling_power,
    
    r.fling_effect->>'name' AS fling_effect_name,
    
    -- Category information
    r.category->>'name' AS category_name,
    
    -- Extract attributes as array for easier querying
    ARRAY(
        SELECT jsonb_array_elements_text(r.attributes)
    ) AS item_attributes,
    
    -- Parse effect text with language preference
    COALESCE(e.effect_en, e.effect_fallback) AS effect_description,
    
    -- Replace sprite URL with constructed URL or NULL
    'https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/items/' || r.name || '.png' AS sprite_url,
    
    -- Derived item classifications
    CASE
        WHEN r.category->>'name' = 'healing' OR r.name LIKE '%potion%' OR r.name = 'full-restore' THEN TRUE
        ELSE FALSE
    END AS is_healing_item,
    
    CASE
        WHEN r.name LIKE '%ball%' AND r.name != 'black-belt' THEN TRUE
        ELSE FALSE
    END AS is_pokeball,
    
    CASE
        WHEN r.category->>'name' = 'battle-items' OR 
             r.name IN ('x-attack', 'x-defense', 'x-speed', 'dire-hit', 'guard-spec') THEN TRUE
        ELSE FALSE
    END AS is_battle_item,
    
    CASE
        WHEN r.name LIKE '%tm%' OR r.name LIKE '%technical-machine%' THEN TRUE
        ELSE FALSE
    END AS is_tm,
    
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(r.attributes) AS attr
            WHERE attr = 'holdable'
        ) THEN TRUE
        ELSE FALSE
    END AS is_holdable,
    
    -- Item tier based on cost and effects
    CASE
        WHEN r.cost::TEXT ~ '^[0-9]+$' AND r.cost::INTEGER > 10000 OR r.name LIKE '%master%ball%' THEN 'Ultra Rare'
        WHEN r.cost::TEXT ~ '^[0-9]+$' AND r.cost::INTEGER > 5000 THEN 'Rare'
        WHEN r.cost::TEXT ~ '^[0-9]+$' AND r.cost::INTEGER > 1000 THEN 'Uncommon'
        ELSE 'Common'
    END AS item_rarity,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data r
LEFT JOIN effect_text e ON r.id = e.id