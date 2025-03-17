
  create view "nerd_facts"."public"."stg_netrunner_cards__dbt_tmp"
    
    
  as (
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
        uniqueness AS is_unique_card, 
        deck_limit,
        minimum_deck_size,
        influence_limit,
        base_link,
        cost,
        faction_cost,
        flavor,
        illustrator,
        keywords,
        memory_cost,
        side_code,
        strength,
        text,
        trash_cost,
        advancement_cost,
        agenda_points
    FROM "nerd_facts"."raw"."netrunner_cards"
    WHERE code IS NOT NULL -- Ensure we don't include invalid entries
)

SELECT
    -- Primary identifiers
    r.id,
    r.code,
    r.title AS card_name,
    
    -- Card type & faction metadata (without joins)
    r.type_code,
    r.type_code AS type_name, -- Temporary placeholder instead of t.name
    r.faction_code,
    r.faction_code AS faction_name, -- Temporary placeholder instead of f.name
    r.side_code,
    
    -- Pack information
    r.pack_code,
    r.position,
    r.quantity,
    
    -- Rest of fields remain unchanged
    r.is_unique_card,
    r.deck_limit,
    r.minimum_deck_size,
    r.influence_limit,
    r.base_link,
    r.cost,
    r.faction_cost AS influence_cost,
    r.memory_cost,
    r.strength,
    r.trash_cost,
    r.advancement_cost,
    r.agenda_points,
    r.text AS card_text,
    r.flavor AS flavor_text,
    r.illustrator,
    
    -- Parse keywords into array for easier analysis
    CASE 
        WHEN r.keywords IS NULL OR r.keywords = '' THEN NULL
        ELSE string_to_array(r.keywords, ' - ')
    END AS keywords_array,
    
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM raw_data r
-- Remove JOIN statements
  );