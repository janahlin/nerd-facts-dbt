/*
  Model: stg_netrunner_cards
  Description: Standardizes Netrunner card data from the raw source with faction and type enrichment
  Sources:
    - raw.netrunner_cards (primary)
    - raw.netrunner_factions (joining for faction names)
    - raw.netrunner_types (joining for type names)
  
  Note: This model combines and replaces the previous stg_netrunner.sql model.
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        code,
        title,
        type_code,
        faction_code,
        pack_code,
        position,
        quantity,
        unique AS is_unique, -- Renamed to avoid SQL reserved word
        deck_limit,
        minimum_deck_size,
        influence_limit,
        base_link,
        cost,
        faction_cost,
        flavor,
        illustrator,
        influence_cost,
        keywords,
        memory_cost,
        side_code,
        strength,
        text,
        trash_cost,
        advancement_cost,
        agenda_points
    FROM {{ source('netrunner', 'cards') }}
    WHERE code IS NOT NULL -- Ensure we don't include invalid entries
)

SELECT
    -- Primary identifiers
    id,
    code,
    title AS card_name, -- Standardized naming convention
    
    -- Card type & faction metadata with enrichment
    type_code,
    t.name AS type_name, -- Added from stg_netrunner.sql
    faction_code,
    f.name AS faction_name, -- Added from stg_netrunner.sql
    side_code,
    
    -- Pack information
    pack_code,
    position,
    quantity,
    
    -- Card characteristics
    is_unique, -- Fixed reserved word issue
    
    -- Deck-building attributes
    NULLIF(deck_limit, 0)::INTEGER AS deck_limit, -- Handle zero values
    NULLIF(minimum_deck_size, 0)::INTEGER AS minimum_deck_size,
    NULLIF(influence_limit, 0)::INTEGER AS influence_limit,
    
    -- Runner-specific attributes
    NULLIF(base_link, '')::INTEGER AS base_link, -- Convert empty strings to NULL
    
    -- Costs & stats
    CASE
        WHEN cost = '' THEN NULL
        WHEN cost = 'X' THEN -1 -- Special case for variable cost
        ELSE NULLIF(cost, '')::INTEGER
    END AS cost,
    
    NULLIF(faction_cost, 0)::INTEGER AS influence_cost, -- Standardized naming
    NULLIF(memory_cost, '')::INTEGER AS memory_cost,
    
    -- Variable card strength (for ICE and programs)
    CASE
        WHEN strength = '' THEN NULL
        WHEN strength = 'X' THEN -1 -- Special case for variable strength
        ELSE NULLIF(strength, '')::NUMERIC
    END AS strength,
    
    NULLIF(trash_cost, '')::INTEGER AS trash_cost,
    
    -- Agenda stats
    CASE
        WHEN advancement_cost IN ('', 'null', 'NaN') THEN NULL
        ELSE NULLIF(advancement_cost, '')::INTEGER
    END AS advancement_requirement,
    
    CASE
        WHEN agenda_points IN ('', 'null', 'NaN') THEN NULL
        ELSE NULLIF(agenda_points, '')::INTEGER
    END AS agenda_points,
    
    -- Card text & art
    text AS card_text,
    flavor AS flavor_text,
    illustrator,
    
    -- Parse keywords into array for easier analysis
    CASE 
        WHEN keywords IS NULL OR keywords = '' THEN NULL
        ELSE string_to_array(keywords, ' - ')
    END AS keywords_array,
    
    -- Card type flags (from stg_netrunner.sql)
    CASE WHEN type_code = 'agenda' THEN TRUE ELSE FALSE END AS is_agenda,
    CASE WHEN type_code = 'ice' THEN TRUE ELSE FALSE END AS is_ice,
    CASE WHEN type_code = 'identity' THEN TRUE ELSE FALSE END AS is_identity,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data r
LEFT JOIN {{ source('netrunner', 'factions') }} f ON r.faction_code = f.code
LEFT JOIN {{ source('netrunner', 'types') }} t ON r.type_code = t.code
