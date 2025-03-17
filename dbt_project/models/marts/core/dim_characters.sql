/*
  Model: dim_characters
  Description: Combined dimension table for characters across fictional universes
  
  Notes:
  - Combines Star Wars characters, Pokémon, and Netrunner identities
  - Standardizes attributes across universes for consistent analysis
  - Adds surrogate keys and proper type handling
  - Includes universe-specific attributes and shared classifications
*/

WITH sw_characters AS (
    SELECT
        'star_wars' AS universe,
        'sw_' || id AS character_source_id,
        name AS character_name,
        species AS species,
        homeworld_id AS home_location_id,
        
        -- Physical attributes
        CASE 
            WHEN height_cm IS NULL THEN NULL  -- This is redundant
            ELSE height_cm
        END AS height_cm,
        
        CASE 
            WHEN mass_kg IS NULL THEN NULL
            ELSE mass_kg
        END AS weight_kg,
        
        -- Demographics
        gender,
        birth_year,
        
        -- Character attributes
        force_sensitive AS has_special_powers,
        force_rating AS power_level,
        'Force' AS power_type,
        
        -- Additional SW-specific attributes
        hair_color,
        eye_color,
        skin_color,
        
        -- Grouping fields
        film_appearances AS appearance_count,
        film_names AS film_appearances_list,
        character_era AS primary_era,
        
        CURRENT_TIMESTAMP AS dbt_loaded_at,
        
        -- Add source tracking information
        created_at AS source_created_at,
        updated_at AS source_updated_at,
        fetch_timestamp
    FROM {{ ref('stg_swapi_people') }}
),

pokemon AS (
    SELECT
        'pokemon' AS universe,
        'pkmn_' || id AS character_source_id,
        name AS character_name,
        primary_type AS species,
        region AS home_location_id,
        
        -- Physical attributes (convert from decimeters to cm)
        height * 10 AS height_cm, 
        weight AS weight_kg,
        
        -- Demographics (not applicable for Pokémon)
        NULL AS gender,
        NULL AS birth_year,
        
        -- Character attributes (all Pokémon have special powers)
        TRUE AS has_special_powers,
        CASE 
            WHEN is_legendary THEN 5
            WHEN is_mythical THEN 5
            WHEN total_base_stats > 580 THEN 4
            WHEN total_base_stats > 500 THEN 3
            WHEN total_base_stats > 400 THEN 2
            ELSE 1
        END AS power_level,
        primary_type AS power_type,
        
        -- Additional Pokémon-specific attributes
        NULL AS hair_color,
        NULL AS eye_color,
        NULL AS skin_color,
        
        -- Grouping fields
        1 AS appearance_count, -- Default for now
        'Generation ' || generation_number AS primary_era,
        
        CURRENT_TIMESTAMP AS dbt_loaded_at,
        
        -- Add source tracking information
        created_at AS source_created_at,
        updated_at AS source_updated_at,
        fetch_timestamp
    FROM {{ ref('stg_pokeapi_pokemon') }}
),

netrunner_identities AS (
    SELECT
        'netrunner' AS universe,
        'nr_' || code AS character_source_id,
        card_name AS character_name,
        faction_name AS species,
        NULL AS home_location_id,
        
        -- Physical attributes (not applicable for Netrunner)
        NULL AS height_cm,
        NULL AS weight_kg,
        
        -- Demographics (special handling for Netrunner)
        CASE 
            WHEN side_name = 'Runner' THEN 'unknown'
            ELSE 'Corporation'
        END AS gender,
        NULL AS birth_year,
        
        -- Character attributes
        TRUE AS has_special_powers,
        CASE
            WHEN influence_limit > 15 THEN 4
            WHEN influence_limit > 12 THEN 3
            ELSE 2
        END AS power_level,
        card_type AS power_type,
        
        -- Additional Netrunner-specific attributes
        NULL AS hair_color,
        NULL AS eye_color,
        NULL AS skin_color,
        
        -- Grouping fields
        1 AS appearance_count, -- Default for now
        NULL AS primary_era,
        
        CURRENT_TIMESTAMP AS dbt_loaded_at,
        
        -- Add source tracking information
        created_at AS source_created_at,
        updated_at AS source_updated_at,
        fetch_timestamp
    FROM {{ ref('stg_netrunner_cards') }}
    WHERE is_identity -- Only include identity cards as "characters"
),

combined_characters AS (
    SELECT * FROM sw_characters
    UNION ALL
    SELECT * FROM pokemon
    UNION ALL
    SELECT * FROM netrunner_identities
)

-- Create final dimension table with surrogate key
SELECT
    {{ dbt_utils.generate_surrogate_key(['universe', 'character_source_id']) }} AS character_key,
    *,
    
    -- Add character classifications
    CASE
        WHEN has_special_powers AND power_level >= 4 THEN 'Legendary'
        WHEN has_special_powers AND power_level >= 3 THEN 'Powerful'
        WHEN has_special_powers THEN 'Gifted'
        ELSE 'Ordinary'
    END AS character_class,
    
    -- Height classification
    CASE
        WHEN height_cm IS NULL THEN 'Unknown'
        WHEN height_cm < 30 THEN 'Tiny'
        WHEN height_cm < 100 THEN 'Small'  
        WHEN height_cm < 180 THEN 'Medium'
        WHEN height_cm < 250 THEN 'Tall'
        ELSE 'Giant'
    END AS height_class,

    -- Add cross-universe comparison field
    CASE 
        WHEN universe = 'star_wars' AND force_sensitive THEN 'Force User'
        WHEN universe = 'star_wars' AND NOT force_sensitive THEN 'Normal Being'
        WHEN universe = 'pokemon' AND is_legendary THEN 'Legendary Creature' 
        WHEN universe = 'pokemon' THEN 'Creature'
        WHEN universe = 'netrunner' AND side_name = 'Runner' THEN 'Digital Entity'
        WHEN universe = 'netrunner' THEN 'Corporate Entity'
        ELSE 'Unknown Entity'
    END AS entity_classification

FROM combined_characters
LEFT JOIN {{ ref('stg_swapi_planets') }} planets
    ON characters.homeworld_id = planets.id