/*
  Model: dim_netrunner_factions
  Description: Dimension table for Android: Netrunner factions
  
  Notes:
  - Provides comprehensive faction information and classifications
  - Includes card counts and distributions across types
  - Adds gameplay style classification and meta position
  - Contains faction metadata and visual styling attributes
*/

WITH faction_base AS (
    SELECT DISTINCT
        f.faction_code,
        f.faction_name,
        f.side_code,
        f.side_name,
        f.is_mini AS is_mini_faction,
        f.color
    FROM {{ ref('stg_netrunner_factions') }} f
),

card_counts AS (
    SELECT
        faction_code,
        COUNT(DISTINCT c.id) AS num_cards,  -- Added c. prefix to resolve ambiguity
        COUNT(DISTINCT CASE WHEN type_name = 'Identity' THEN c.id END) AS num_identities,
        COUNT(DISTINCT CASE WHEN type_name = 'ICE' THEN c.id END) AS num_ice,
        COUNT(DISTINCT CASE WHEN type_name = 'Program' AND card_text ILIKE '%Icebreaker%' THEN c.id END) AS num_icebreakers,
        COUNT(DISTINCT CASE WHEN type_name = 'Agenda' THEN c.id END) AS num_agendas,
        COUNT(DISTINCT CASE WHEN type_name IN ('Event', 'Operation') THEN c.id END) AS num_events_operations,
        COUNT(DISTINCT CASE WHEN card_text ILIKE '%gain%credit%' OR 
                              card_text ILIKE '%take%credit%' OR
                              card_text ILIKE '%credit for each%' THEN c.id END) AS num_economy_cards,
        -- Add first release date
        MIN(p.release_date) AS first_release_date,
        -- Add most recent card release date
        MAX(p.release_date) AS latest_release_date
    FROM {{ ref('stg_netrunner_cards') }} c
    LEFT JOIN {{ ref('stg_netrunner_packs') }} p ON c.pack_code = p.code
    GROUP BY faction_code
)

SELECT
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['f.faction_code']) }} AS faction_key,
    
    -- Primary identifiers
    f.faction_code,
    f.faction_name,
    f.side_code,
    f.side_name,
    f.is_mini_faction,
    f.color,
    
    -- Card statistics
    COALESCE(c.num_cards, 0) AS num_cards,
    COALESCE(c.num_identities, 0) AS num_identities,
    COALESCE(c.num_ice, 0) AS num_ice,
    COALESCE(c.num_icebreakers, 0) AS num_icebreakers,
    COALESCE(c.num_agendas, 0) AS num_agendas,
    COALESCE(c.num_events_operations, 0) AS num_events_operations,
    COALESCE(c.num_economy_cards, 0) AS num_economy_cards,
    
    -- Dates
    c.first_release_date,
    c.latest_release_date,
    
    -- Faction tier based on card count with improved thresholds
    CASE
        WHEN COALESCE(c.num_cards, 0) > 120 THEN 'Major'
        WHEN COALESCE(c.num_cards, 0) > 70 THEN 'Standard'
        WHEN COALESCE(c.num_cards, 0) > 20 THEN 'Minor'
        ELSE 'Mini'
    END AS faction_tier,
    
    -- Faction type based on side and characteristics with improved pattern matching
    CASE
        WHEN f.side_name = 'Corp' AND f.faction_name ILIKE '%jinteki%' THEN 'Trap Corp'
        WHEN f.side_name = 'Corp' AND f.faction_name ILIKE '%haas-bioroid%' THEN 'Economy Corp'
        WHEN f.side_name = 'Corp' AND f.faction_name ILIKE '%nbn%' THEN 'Tag Corp'
        WHEN f.side_name = 'Corp' AND f.faction_name ILIKE '%weyland%' THEN 'Damage Corp'
        WHEN f.side_name = 'Runner' AND f.faction_name ILIKE '%anarch%' THEN 'Disruption Runner'
        WHEN f.side_name = 'Runner' AND f.faction_name ILIKE '%criminal%' THEN 'Economy Runner'
        WHEN f.side_name = 'Runner' AND f.faction_name ILIKE '%shaper%' THEN 'Rig-Builder Runner'
        ELSE 'Specialized'
    END AS play_style,
    
    -- Economic strength based on economy card percentage
    CASE
        WHEN COALESCE(c.num_cards, 0) = 0 THEN 'Unknown'
        WHEN COALESCE(c.num_economy_cards, 0) * 100.0 / NULLIF(c.num_cards, 0) >= 20 THEN 'Strong Economy'
        WHEN COALESCE(c.num_economy_cards, 0) * 100.0 / NULLIF(c.num_cards, 0) >= 15 THEN 'Good Economy'
        WHEN COALESCE(c.num_economy_cards, 0) * 100.0 / NULLIF(c.num_cards, 0) >= 10 THEN 'Moderate Economy'
        ELSE 'Weak Economy'
    END AS economy_strength,
    
    -- Iconic identity card for this faction with more comprehensive matching
    CASE
        WHEN f.faction_code = 'jinteki' THEN 'Personal Evolution'
        WHEN f.faction_code = 'haas-bioroid' THEN 'Engineering the Future'
        WHEN f.faction_code = 'nbn' THEN 'Making News'
        WHEN f.faction_code = 'weyland-consortium' THEN 'Building a Better World'
        WHEN f.faction_code = 'anarch' THEN 'Noise'
        WHEN f.faction_code = 'criminal' THEN 'Gabriel Santiago'
        WHEN f.faction_code = 'shaper' THEN 'Kate "Mac" McCaffrey'
        WHEN f.faction_code = 'adam' THEN 'Adam: Compulsive Hacker'
        WHEN f.faction_code = 'apex' THEN 'Apex: Invasive Predator'
        WHEN f.faction_code = 'sunny-lebeau' THEN 'Sunny Lebeau: Security Specialist'
        ELSE 'Various'
    END AS iconic_identity,
    
    -- Release wave with expanded categories
    CASE
        WHEN f.faction_code IN ('haas-bioroid', 'jinteki', 'nbn', 'weyland-consortium', 'anarch', 'criminal', 'shaper') THEN 'Core Set'
        WHEN f.faction_code IN ('adam', 'apex', 'sunny-lebeau') THEN 'Data and Destiny'
        ELSE 'Expansion'
    END AS release_category,
    
    -- Meta position
    CASE
        WHEN f.faction_code IN ('nbn', 'haas-bioroid', 'criminal', 'shaper') THEN 'Tier 1'
        WHEN f.faction_code IN ('jinteki', 'weyland-consortium', 'anarch') THEN 'Tier 2'
        ELSE 'Tier 3'
    END AS meta_position,
    
    -- CSS classes for styling
    'faction-' || f.faction_code AS faction_css_class,
    
    -- Add hex color code with # prefix if not already present
    CASE
        WHEN f.color IS NULL THEN '#000000'  -- Default black
        WHEN f.color LIKE '#%' THEN f.color
        ELSE '#' || f.color
    END AS color_hex,
    
    -- Faction abbreviation
    CASE
        WHEN f.faction_code = 'haas-bioroid' THEN 'HB'
        WHEN f.faction_code = 'jinteki' THEN 'J'
        WHEN f.faction_code = 'nbn' THEN 'NBN'
        WHEN f.faction_code = 'weyland-consortium' THEN 'W'
        WHEN f.faction_code = 'anarch' THEN 'A'
        WHEN f.faction_code = 'criminal' THEN 'C'
        WHEN f.faction_code = 'shaper' THEN 'S'
        WHEN f.faction_code = 'adam' THEN 'Adam'
        WHEN f.faction_code = 'apex' THEN 'Apex'
        WHEN f.faction_code = 'sunny-lebeau' THEN 'Sunny'
        ELSE SUBSTRING(f.faction_name, 1, 1)
    END AS faction_abbr,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM faction_base f
LEFT JOIN card_counts c ON f.faction_code = c.faction_code
ORDER BY f.side_name, COALESCE(c.num_cards, 0) DESC