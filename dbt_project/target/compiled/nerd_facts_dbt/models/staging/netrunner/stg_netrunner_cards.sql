/*
  Model: stg_netrunner_cards
  Description: Standardizes Netrunner card data from the raw source with faction and type enrichment

  
  Note: This model combines and replaces the previous stg_netrunner.sql model.
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        -- Primary identifiers
        id,

        -- Text fields
        code,
        flavor,
        title,
        type_code,
        faction_code,
        pack_code,
        illustrator,
        keywords,        
        side_code,        
        text,                      
        uniqueness, 
        stripped_text,
        stripped_title,

        -- Numeric fields
        CASE WHEN agenda_points~E'^[0-9]+$' THEN agenda_points ELSE NULL END AS agenda_points,
        CASE WHEN position~E'^[0-9]+$' THEN position ELSE NULL END AS position,
        CASE WHEN quantity~E'^[0-9]+$' THEN quantity ELSE NULL END AS quantity,
        CASE WHEN deck_limit~E'^[0-9]+$' THEN deck_limit ELSE NULL END AS deck_limit,
        CASE WHEN minimum_deck_size~E'^[0-9]+$' THEN minimum_deck_size ELSE NULL END AS minimum_deck_size,
        CASE WHEN memory_cost~E'^[0-9]+$' THEN memory_cost ELSE NULL END AS memory_cost,
        CASE WHEN influence_limit~E'^[0-9]+$' THEN influence_limit ELSE NULL END AS influence_limit,
        CASE WHEN strength~E'^[0-9]+$' THEN strength ELSE NULL END AS strength,
        CASE WHEN base_link~E'^[0-9]+$' THEN base_link ELSE NULL END AS base_link,
        CASE WHEN cost~E'^[0-9]+$' THEN cost ELSE NULL END AS cost,
        CASE WHEN trash_cost~E'^[0-9]+$' THEN trash_cost ELSE NULL END AS trash_cost,
        CASE WHEN faction_cost~E'^[0-9]+$' THEN faction_cost ELSE NULL END AS faction_cost,
        CASE WHEN advancement_cost~E'^[0-9]+$' THEN advancement_cost ELSE NULL END AS advancement_cost        
        
    FROM "nerd_facts"."raw"."netrunner_cards"
    WHERE code IS NOT NULL -- Ensure we don't include invalid entries
)

SELECT    
    -- Primary identifiers
    id as card_id,

    -- Text fields
    code,
    flavor,
    title as card_name,
    type_code,
    faction_code,
    pack_code,
    illustrator,
    keywords,        
    side_code,        
    text,                      
    uniqueness, 
    stripped_text,
    stripped_title,

    -- Numeric fields
    CAST(agenda_points AS NUMERIC) AS agenda_points,
    CAST(position AS NUMERIC) AS position,
    CAST(quantity AS NUMERIC) AS quantity,
    CAST(deck_limit AS NUMERIC) AS deck_limit,
    CAST(minimum_deck_size AS NUMERIC) AS minimum_deck_size,
    CAST(memory_cost AS NUMERIC) AS memory_cost,
    CAST(influence_limit AS NUMERIC) AS influence_limit,
    CAST(strength AS NUMERIC) AS strength,
    CAST(base_link AS NUMERIC) AS base_link,
    CAST(cost AS NUMERIC) AS cost,
    CAST(trash_cost AS NUMERIC) AS trash_cost,
    CAST(faction_cost AS NUMERIC) AS faction_cost,
    CAST(advancement_cost AS NUMERIC) AS advancement_cost,

    -- Add data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at  
        
FROM raw_data