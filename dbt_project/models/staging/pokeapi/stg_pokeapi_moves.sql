/*
  Model: stg_pokeapi_moves
  Description: Standardizes Pokémon move data from the PokeAPI
  Source: raw.pokeapi_moves
  
  Notes:
  - Effect entries are extracted with language preference for English entries
  - Move classifications are derived from damage class, type and effect text
  - NULL handling is added for all numeric fields and JSON extraction
  - Additional move statistics calculated for battle analysis
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        generation,
        type,
        damage_class,
        power,
        pp,
        accuracy,
        priority,
        target,
        effect_entries,
        effect_chance,
        meta,
        stat_changes,
        contest_type,
        contest_effect
    FROM raw.pokeapi_moves
    WHERE id IS NOT NULL
),

-- Extract English effect text with fallbacks
effect_text AS (
    SELECT
        id,
        (
            -- First try to find an English entry
            SELECT effect_entry->>'effect'
            FROM jsonb_array_elements(effect_entries) AS effect_entry
            WHERE effect_entry->>'language'->>'name' = 'en'
            LIMIT 1
        ) AS effect_en,
        (
            -- First try to find an English short entry
            SELECT effect_entry->>'short_effect'
            FROM jsonb_array_elements(effect_entries) AS effect_entry
            WHERE effect_entry->>'language'->>'name' = 'en'
            LIMIT 1
        ) AS short_effect_en,
        -- Fallbacks to first entries
        COALESCE(effect_entries->0->>'effect', '') AS effect_fallback,
        COALESCE(effect_entries->0->>'short_effect', '') AS short_effect_fallback
    FROM raw_data
)

SELECT
    -- Primary identifiers
    r.id,
    r.name AS move_name,
    
    -- Basic move attributes
    r.generation->>'name' AS generation,
    r.type->>'name' AS move_type,
    r.damage_class->>'name' AS damage_class,
    
    -- Convert empty/zero values to NULL for numeric fields
    NULLIF(r.power, 0)::INTEGER AS power,
    NULLIF(r.pp, 0)::INTEGER AS pp,
    NULLIF(r.accuracy, 0)::INTEGER AS accuracy,
    r.priority::INTEGER AS priority,
    
    -- Target information
    r.target->>'name' AS target,
    
    -- Effect text with language preference
    COALESCE(e.short_effect_en, e.short_effect_fallback) AS short_effect,
    COALESCE(e.effect_en, e.effect_fallback) AS effect_description,
    NULLIF(r.effect_chance, 0)::INTEGER AS effect_chance,
    
    -- Meta data extraction with NULL handling
    COALESCE(r.meta->>'ailment'->>'name', 'none') AS status_effect,
    NULLIF(r.meta->>'min_hits', 0)::INTEGER AS min_hits,
    NULLIF(r.meta->>'max_hits', 0)::INTEGER AS max_hits,
    NULLIF(r.meta->>'drain', 0)::INTEGER AS hp_drain_percent,
    NULLIF(r.meta->>'healing', 0)::INTEGER AS healing_percent,
    NULLIF(r.meta->>'crit_rate', 0)::INTEGER AS critical_hit_rate,
    
    -- Contest information
    r.contest_type->>'name' AS contest_type,
    
    -- Move classification flags
    CASE
        WHEN r.damage_class->>'name' = 'physical' THEN TRUE
        ELSE FALSE
    END AS is_physical,
    
    CASE
        WHEN r.damage_class->>'name' = 'special' THEN TRUE
        ELSE FALSE
    END AS is_special,
    
    CASE
        WHEN r.damage_class->>'name' = 'status' THEN TRUE
        ELSE FALSE
    END AS is_status,
    
    -- Derived move characteristics
    CASE
        WHEN r.power IS NULL OR r.power = 0 THEN FALSE
        ELSE TRUE
    END AS deals_damage,
    
    CASE
        WHEN LOWER(COALESCE(e.effect_en, e.effect_fallback)) LIKE '%heal%' OR
             COALESCE(r.meta->>'healing', 0) > 0 THEN TRUE
        ELSE FALSE
    END AS has_healing_effect,
    
    -- Move power tier
    CASE
        WHEN r.power >= 150 THEN 'Ultra Powerful'
        WHEN r.power >= 100 THEN 'Very Powerful'
        WHEN r.power >= 80 THEN 'Powerful'
        WHEN r.power >= 60 THEN 'Moderate'
        WHEN r.power >= 40 THEN 'Weak'
        WHEN r.power > 0 THEN 'Very Weak'
        ELSE 'No Direct Damage'
    END AS power_tier,
    
    -- Accuracy rating
    CASE
        WHEN r.accuracy IS NULL THEN 'Always Hits'
        WHEN r.accuracy >= 95 THEN 'Very Accurate'
        WHEN r.accuracy >= 80 THEN 'Accurate'
        WHEN r.accuracy >= 70 THEN 'Moderately Accurate'
        ELSE 'Inaccurate'
    END AS accuracy_tier,
    
    -- Calculate approximate move DPS (Damage Per Second) for comparison
    CASE
        WHEN r.power > 0 AND r.accuracy > 0 
        THEN ROUND((r.power * r.accuracy / 100.0)::NUMERIC, 1)
        ELSE 0
    END AS effective_power,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data r
LEFT JOIN effect_text e ON r.id = e.id