/*
  Model: stg_pokeapi_types
  Description: Standardizes Pokémon type data from the PokeAPI
  Source: raw.pokeapi_types
  
  Notes:
  - Damage relations are extracted from nested JSON structure
  - Fixed set-returning function issues in COALESCE
  - Added comprehensive type effectiveness metrics
  - Added data quality checks
  - Fixed JSON type casting for array length calculations
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        damage_relations::JSONB AS damage_relations,
        pokemon::JSONB AS pokemon,  -- Cast to JSONB explicitly
        moves::JSONB AS moves       -- Cast to JSONB explicitly
    FROM raw.pokeapi_types
    WHERE id IS NOT NULL
),

-- Extract type relationships into rows for easier querying
damage_relations_unpacked AS (
    SELECT
        id,
        name,
        -- Types that are super effective against this type (this type takes 2x damage)
        ARRAY(
            SELECT rel->>'name'
            FROM jsonb_array_elements(damage_relations->'double_damage_from') AS rel
        ) AS weak_against,
        
        -- Types that this type is super effective against (this type deals 2x damage)
        ARRAY(
            SELECT rel->>'name'
            FROM jsonb_array_elements(damage_relations->'double_damage_to') AS rel
        ) AS strong_against,
        
        -- Types that are not very effective against this type (this type takes 0.5x damage)
        ARRAY(
            SELECT rel->>'name'
            FROM jsonb_array_elements(damage_relations->'half_damage_from') AS rel
        ) AS resistant_to,
        
        -- Types that this type is not very effective against (this type deals 0.5x damage)
        ARRAY(
            SELECT rel->>'name'
            FROM jsonb_array_elements(damage_relations->'half_damage_to') AS rel
        ) AS weak_damage_to,
        
        -- Types that have no effect on this type (this type takes 0x damage)
        ARRAY(
            SELECT rel->>'name'
            FROM jsonb_array_elements(damage_relations->'no_damage_from') AS rel
        ) AS immune_to,
        
        -- Types that this type has no effect on (this type deals 0x damage)
        ARRAY(
            SELECT rel->>'name'
            FROM jsonb_array_elements(damage_relations->'no_damage_to') AS rel
        ) AS no_effect_on,
        
        -- Count of Pokémon with this type - with proper JSONB casting
        COALESCE(jsonb_array_length(pokemon), 0) AS pokemon_count,
        
        -- Count of moves with this type - with proper JSONB casting
        COALESCE(jsonb_array_length(moves), 0) AS move_count
    FROM raw_data
)

SELECT
    -- Primary identifiers
    id,
    name AS type_name,
    
    -- Type relationships
    weak_against,
    strong_against,
    resistant_to,
    weak_damage_to,
    immune_to,
    no_effect_on,
    
    -- Counts and statistics
    COALESCE(array_length(weak_against, 1), 0) AS weakness_count,
    COALESCE(array_length(strong_against, 1), 0) AS strength_count,
    COALESCE(array_length(resistant_to, 1), 0) AS resistance_count,
    COALESCE(array_length(immune_to, 1), 0) AS immunity_count,
    
    -- Type effectiveness metrics - higher means better defensive type
    COALESCE(array_length(resistant_to, 1), 0) + 
    COALESCE(array_length(immune_to, 1) * 2, 0) - 
    COALESCE(array_length(weak_against, 1), 0) AS defensive_score,
    
    -- Type effectiveness metrics - higher means better offensive type
    COALESCE(array_length(strong_against, 1), 0) - 
    COALESCE(array_length(weak_damage_to, 1), 0) - 
    COALESCE(array_length(no_effect_on, 1) * 2, 0) AS offensive_score,
    
    -- Pokemon and move counts
    pokemon_count,
    move_count,
    
    -- Type color mapping for visualizations
    CASE
        WHEN name = 'normal' THEN '#A8A878'
        WHEN name = 'fighting' THEN '#C03028'
        WHEN name = 'flying' THEN '#A890F0'
        WHEN name = 'poison' THEN '#A040A0'
        WHEN name = 'ground' THEN '#E0C068'
        WHEN name = 'rock' THEN '#B8A038'
        WHEN name = 'bug' THEN '#A8B820'
        WHEN name = 'ghost' THEN '#705898'
        WHEN name = 'steel' THEN '#B8B8D0'
        WHEN name = 'fire' THEN '#F08030'
        WHEN name = 'water' THEN '#6890F0'
        WHEN name = 'grass' THEN '#78C850'
        WHEN name = 'electric' THEN '#F8D030'
        WHEN name = 'psychic' THEN '#F85888'
        WHEN name = 'ice' THEN '#98D8D8'
        WHEN name = 'dragon' THEN '#7038F8'
        WHEN name = 'dark' THEN '#705848'
        WHEN name = 'fairy' THEN '#EE99AC'
        ELSE '#68A090' -- Default (unknown)
    END AS type_color,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM damage_relations_unpacked