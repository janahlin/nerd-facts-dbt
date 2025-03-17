/*
  Model: dim_netrunner_cards
  Description: Comprehensive dimension table for Android: Netrunner cards
  
  Notes:
  - Combines card data with faction information
  - Creates categorizations and derived attributes for analysis
  - Adds enhanced card typing and sub-typing
  - Includes power level calculations and economy classification
  - Consolidates core metadata from multiple sources
*/

WITH card_base AS (
    SELECT
        c.code AS card_code,
        c.id AS card_id,
        c.card_name,  -- Updated from 'title' based on staging model improvements
        c.type_name,
        c.faction_code,
        c.faction_name, -- Already included from improved stg_netrunner_cards
        c.side_code,
        c.side_name,    -- Included from improved model
        c.influence_cost,
        c.cost,
        c.advancement_requirement,
        c.agenda_points,
        c.strength,
        c.card_text,    -- Updated from 'text' based on staging model
        c.flavor_text,  -- Updated from 'flavor' based on staging model
        c.keywords,
        c.keywords_array, -- Already parsed in stg_netrunner_cards
        c.illustrator,
        c.pack_code,
        c.is_unique,    -- Added from stg_netrunner_cards
        c.deck_limit,   -- Added from stg_netrunner_cards
        c.memory_cost,  -- Added from stg_netrunner_cards 
        c.trash_cost    -- Added from stg_netrunner_cards
    FROM {{ ref('stg_netrunner_cards') }} c
    -- Remove JOIN to factions as it's now included in stg_netrunner_cards
),

-- Add cycle information for release context
card_with_cycle AS (
    SELECT
        c.*,
        p.cycle_code,
        p.cycle_name,
        p.pack_name,
        p.release_date,
        p.pack_position
    FROM card_base c
    LEFT JOIN {{ ref('stg_netrunner_packs') }} p ON c.pack_code = p.code
),

-- Add popularity/usage data (could come from tournament data in real implementation)
card_usage AS (
    SELECT
        card_code,
        -- Simulated usage metrics - in a real implementation, join to actual tournament data
        CASE
            WHEN card_name IN ('Account Siphon', 'Desperado', 'SanSan City Grid', 
                            'Astroscript Pilot Program', 'Sure Gamble', 'Hedge Fund') THEN 80
            WHEN cycle_code IN ('core', 'genesis', 'creation-and-control') AND influence_cost >= 3 THEN 65  
            WHEN cycle_code IN ('core', 'genesis') AND influence_cost >= 2 THEN 50
            WHEN cycle_code NOT IN ('core', 'genesis', 'creation-and-control') THEN 30
            ELSE 40
        END AS usage_percentage
    FROM card_with_cycle
)

SELECT
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['c.card_code']) }} AS card_key,
    
    -- Primary identifiers
    c.card_code,
    c.card_id,
    c.card_name,
    
    -- Card type and classification
    c.type_name,
    c.faction_code,
    c.faction_name,
    c.side_code,
    c.side_name,
    
    -- Card attributes
    c.influence_cost,
    c.cost,
    c.advancement_requirement,
    c.agenda_points,
    c.strength,
    c.memory_cost,
    c.trash_cost,
    c.is_unique,
    c.deck_limit,
    
    -- Card text and art
    c.card_text,
    c.flavor_text,
    c.keywords,
    c.keywords_array,
    c.illustrator,
    
    -- Publication data
    c.pack_code,
    c.pack_name,
    c.cycle_code,
    c.cycle_name,
    c.release_date,
    
    -- Card rarity (improved logic)
    CASE
        WHEN c.pack_code = 'core' THEN
            CASE
                WHEN c.influence_cost >= 4 OR c.card_name IN ('SanSan City Grid', 'Desperado') THEN '1x (Rare)'
                WHEN c.influence_cost >= 2 OR c.card_name IN ('Hedge Fund', 'Sure Gamble') THEN '2x (Uncommon)'
                ELSE '3x (Common)'
            END
        WHEN c.pack_code LIKE 'draft%' THEN 'Draft'
        WHEN c.cycle_code = 'core2' THEN 
            CASE 
                WHEN c.card_name IN ('Sure Gamble', 'Hedge Fund') THEN '3x (Common)'
                WHEN c.is_unique = TRUE THEN '1x (Rare)'
                ELSE '2x (Uncommon)'
            END
        ELSE '3x (Common)'  -- Default for normal packs
    END AS core_rarity,
    
    -- Enhanced card category with subtyping
    CASE
        WHEN c.type_name = 'Identity' THEN 'Identity'
        WHEN c.type_name IN ('Event', 'Operation') THEN 'One-time'
        WHEN c.type_name IN ('Hardware', 'Resource', 'Asset', 'Upgrade') THEN 'Permanent'
        WHEN c.type_name = 'Program' AND c.card_text ILIKE '%Icebreaker%' THEN 
            CASE 
                WHEN c.card_text ILIKE '%Fracter%' THEN 'Icebreaker - Fracter'
                WHEN c.card_text ILIKE '%Decoder%' THEN 'Icebreaker - Decoder'
                WHEN c.card_text ILIKE '%Killer%' THEN 'Icebreaker - Killer'
                WHEN c.card_text ILIKE '%AI%' THEN 'Icebreaker - AI'
                ELSE 'Icebreaker - Other'
            END
        WHEN c.type_name = 'Program' THEN 'Program'
        WHEN c.type_name = 'ICE' THEN 
            CASE
                WHEN c.card_text ILIKE '%Barrier%' AND c.card_text ILIKE '%Code Gate%' AND c.card_text ILIKE '%Sentry%' THEN 'ICE - Mythic'
                WHEN c.card_text ILIKE '%Barrier%' AND c.card_text ILIKE '%Code Gate%' THEN 'ICE - Barrier/Code Gate'
                WHEN c.card_text ILIKE '%Barrier%' AND c.card_text ILIKE '%Sentry%' THEN 'ICE - Barrier/Sentry'
                WHEN c.card_text ILIKE '%Code Gate%' AND c.card_text ILIKE '%Sentry%' THEN 'ICE - Code Gate/Sentry'
                WHEN c.card_text ILIKE '%Barrier%' THEN 'ICE - Barrier'
                WHEN c.card_text ILIKE '%Code Gate%' THEN 'ICE - Code Gate'
                WHEN c.card_text ILIKE '%Sentry%' THEN 'ICE - Sentry'
                WHEN c.card_text ILIKE '%Trap%' THEN 'ICE - Trap'
                ELSE 'ICE - Other'
            END
        WHEN c.type_name = 'Agenda' THEN 'Agenda'
        ELSE 'Other'
    END AS card_category,
    
    -- Card power level (enhanced with more indicators)
    CASE
        -- Powerful/banned cards
        WHEN c.card_name IN ('Adjusted Chronotype', 'Apocalypse', 'Bio-Ethics Association', 
                          'Brain Rewiring', 'Clone Chip', 'Employee Strike', 'NAPD Most Wanted',
                          'Rumor Mill', 'Şifr', 'Temüjin Contract', 'Violet Level Clearance',
                          'Tapwrm', 'Watch the World Burn', 'Aesop\'s Pawnshop') THEN 5
                          
        -- Top meta cards
        WHEN c.card_name IN ('Account Siphon', 'Corroder', 'Hedge Fund', 'Sure Gamble', 
                           'Astroscript Pilot Program', 'SanSan City Grid', 'Desperado',
                           'Daily Casts', 'Jackson Howard', 'Breaking News', 'Eli 1.0',
                           'Parasite', 'Gordian Blade', 'Liberated Account') THEN 5
                           
        -- Known good cards                   
        WHEN (c.type_name = 'ICE' AND COALESCE(c.cost, 0) >= 4) OR 
             (c.type_name = 'Program' AND c.card_text ILIKE '%Icebreaker%' AND COALESCE(c.influence_cost, 0) >= 3) OR
             (c.type_name = 'Agenda' AND COALESCE(c.agenda_points, 0) >= 3 AND COALESCE(c.advancement_requirement, 0) <= 5) OR
             (u.usage_percentage >= 60) THEN 4
             
        -- Average useful cards
        WHEN (COALESCE(c.cost, 0) >= 3) OR 
             (COALESCE(c.influence_cost, 0) >= 2) OR
             (u.usage_percentage >= 40) THEN 3
             
        -- Basic cards
        ELSE 2
    END AS power_level,
    
    -- Economic classification with improved detection
    CASE
        WHEN c.card_text ILIKE '%gain %credit%' OR 
             c.card_text ILIKE '%take %credit%' OR
             c.card_text ILIKE '%credit for each%' OR
             c.card_text ILIKE '%credits when%' THEN 
            CASE
                WHEN c.card_text ILIKE '%when successful%' THEN 'Conditional Economy'
                WHEN c.card_text ILIKE '%trash%' THEN 'One-time Economy'
                WHEN c.type_name IN ('Event', 'Operation') THEN 'Burst Economy'
                ELSE 'Drip Economy'
            END
        ELSE NULL
    END AS economy_type,
    
    -- Enhanced economy card detection
    CASE
        WHEN c.card_text ILIKE '%gain %credit%' OR 
             c.card_text ILIKE '%take %credit%' OR
             c.card_text ILIKE '%credit for each%' OR 
             c.card_text ILIKE '%credits when%' OR
             c.card_name IN ('Hedge Fund', 'Sure Gamble', 'Daily Casts', 'Adonis Campaign',
                           'Magnum Opus', 'PAD Campaign', 'Liberated Account',
                           'Professional Contacts', 'Dirty Laundry', 'Peace in Our Time') THEN TRUE
        ELSE FALSE
    END AS is_economy_card,
    
    -- Card age and usage metrics
    DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.release_date) AS card_age_years,
    u.usage_percentage,
    
    -- Data tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM card_with_cycle c
LEFT JOIN card_usage u ON c.card_code = u.card_code
ORDER BY c.side_name, c.faction_name, c.type_name, c.cost