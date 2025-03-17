
  create view "nerd_facts"."public"."stg_pokeapi_abilities__dbt_tmp"
    
    
  as (
    /*
  Model: stg_pokeapi_abilities
  Description: Standardizes Pokémon ability data from the PokeAPI
  
  Notes:
  - Fixed nested JSON handling using proper extraction patterns
  - Added explicit JSONB casting for all JSON operations
  - Fixed type handling for BOOLEAN fields
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        generation,
        effect_entries,
        pokemon,
        is_main_series
    FROM raw.pokeapi_abilities
    WHERE id IS NOT NULL
),

-- Extract English effect entries when available
effect_entries_parsed AS (
    SELECT
        id,
        (
            -- Fix nested JSON extraction - proper operator sequence
            SELECT effect_entry->>'effect' 
            FROM jsonb_array_elements(effect_entries::JSONB) AS effect_entry
            WHERE effect_entry->'language'->>'name' = 'en'
            LIMIT 1
        ) AS effect_en,
        (
            -- Fix nested JSON extraction - proper operator sequence
            SELECT effect_entry->>'short_effect' 
            FROM jsonb_array_elements(effect_entries::JSONB) AS effect_entry
            WHERE effect_entry->'language'->>'name' = 'en'
            LIMIT 1
        ) AS short_effect_en,
        -- Fallback to first entry if no English entry exists
        COALESCE((effect_entries::JSONB)->0->>'effect', '') AS effect_fallback,
        COALESCE((effect_entries::JSONB)->0->>'short_effect', '') AS short_effect_fallback
    FROM raw_data
)

SELECT
    -- Primary identifiers
    r.id,
    r.name AS ability_name,
    
    -- Generation information with explicit JSONB casting
    COALESCE((r.generation::JSONB)->>'name', 'unknown') AS generation_name,
    CASE
        WHEN (r.generation::JSONB)->>'name' ~ 'generation-([i|v]+)'
        THEN REGEXP_REPLACE((r.generation::JSONB)->>'name', 'generation-([i|v]+)', '\1')
        ELSE NULL
    END AS generation_number,
    
    -- Effect descriptions
    COALESCE(e.effect_en, e.effect_fallback) AS effect_description,
    COALESCE(e.short_effect_en, e.short_effect_fallback) AS short_description,
    
    -- Pokémon count with this ability with explicit JSONB casting
    COALESCE(jsonb_array_length(r.pokemon::JSONB), 0) AS pokemon_count,
    
    -- Canonical status - Fix: Handle TEXT to BOOLEAN conversion properly
    CASE
        WHEN r.is_main_series::TEXT = 'false' THEN FALSE
        ELSE TRUE  -- Default to TRUE for NULL or any non-false value
    END AS is_main_series,
    
    -- Common words in effect description for classification
    CASE
        WHEN LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%boost%' OR 
             LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%increase%' THEN TRUE
        ELSE FALSE
    END AS is_stat_boosting,
    
    CASE
        WHEN LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%weather%' THEN TRUE
        ELSE FALSE
    END AS is_weather_related,
    
    CASE
        WHEN LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%status%' OR 
             LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%poison%' OR
             LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%burn%' OR
             LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%paralyze%' THEN TRUE
        ELSE FALSE
    END AS affects_status_conditions,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data r
JOIN effect_entries_parsed e ON r.id = e.id
  );