/*
  Model: stg_pokeapi_types
  Description: Standardizes Pokémon type data from the PokeAPI
  Source: raw.pokeapi_types
  
  Notes:
  - Type effectiveness relationships are extracted from nested JSON
  - Arrays are converted from JSON to SQL arrays for easier querying
  - Type classifications and attributes are derived
  - Generation information is properly extracted
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        generation,
        damage_relations,
        move_damage_class
    FROM raw.pokeapi_types
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name AS type_name,
    
    -- Generation information
    COALESCE(generation->>'name', 'unknown') AS generation_name,
    CASE
        WHEN generation->>'name' ~ 'generation-([i|v]+)'
        THEN REGEXP_REPLACE(generation->>'name', 'generation-([i|v]+)', '\1')
        ELSE NULL
    END AS generation_number,
    
    -- Damage class (physical, special, etc)
    move_damage_class->>'name' AS damage_class,
    
    -- Type effectiveness relationships - convert JSON arrays to SQL arrays
    ARRAY(
        SELECT COALESCE(jsonb_array_elements(damage_relations->'double_damage_from')->>'name', 'unknown')
    ) AS weaknesses,
    
    ARRAY(
        SELECT COALESCE(jsonb_array_elements(damage_relations->'double_damage_to')->>'name', 'unknown')
    ) AS strengths,
    
    ARRAY(
        SELECT COALESCE(jsonb_array_elements(damage_relations->'half_damage_from')->>'name', 'unknown')
    ) AS resistances,
    
    ARRAY(
        SELECT COALESCE(jsonb_array_elements(damage_relations->'half_damage_to')->>'name', 'unknown')
    ) AS vulnerabilities,
    
    ARRAY(
        SELECT COALESCE(jsonb_array_elements(damage_relations->'no_damage_from')->>'name', 'unknown')
    ) AS immune_from,
    
    ARRAY(
        SELECT COALESCE(jsonb_array_elements(damage_relations->'no_damage_to')->>'name', 'unknown')
    ) AS immune_to,
    
    -- Array lengths for quick counts
    COALESCE(jsonb_array_length(damage_relations->'double_damage_from'), 0) AS weakness_count,
    COALESCE(jsonb_array_length(damage_relations->'double_damage_to'), 0) AS strength_count,
    COALESCE(jsonb_array_length(damage_relations->'no_damage_from'), 0) AS immunity_count,
    
    -- Derived type classifications
    CASE
        WHEN name IN ('fire', 'water', 'grass', 'electric', 'ice') THEN 'Elemental'
        WHEN name IN ('normal', 'fighting', 'flying', 'ground', 'rock') THEN 'Physical'
        WHEN name IN ('psychic', 'ghost', 'dark', 'fairy') THEN 'Special'
        WHEN name IN ('poison', 'bug', 'dragon', 'steel') THEN 'Other'
        ELSE 'Unknown'
    END AS type_category,
    
    -- Type color for UI displays
    CASE
        WHEN name = 'normal' THEN '#A8A77A'
        WHEN name = 'fire' THEN '#EE8130'
        WHEN name = 'water' THEN '#6390F0'
        WHEN name = 'electric' THEN '#F7D02C'
        WHEN name = 'grass' THEN '#7AC74C'
        WHEN name = 'ice' THEN '#96D9D6'
        WHEN name = 'fighting' THEN '#C22E28'
        WHEN name = 'poison' THEN '#A33EA1'
        WHEN name = 'ground' THEN '#E2BF65'
        WHEN name = 'flying' THEN '#A98FF3'
        WHEN name = 'psychic' THEN '#F95587'
        WHEN name = 'bug' THEN '#A6B91A'
        WHEN name = 'rock' THEN '#B6A136'
        WHEN name = 'ghost' THEN '#735797'
        WHEN name = 'dragon' THEN '#6F35FC'
        WHEN name = 'dark' THEN '#705746'
        WHEN name = 'steel' THEN '#B7B7CE'
        WHEN name = 'fairy' THEN '#D685AD'
        ELSE '#CCCCCC'
    END AS type_color,
    
    -- Calculate offensive and defensive scores (higher = better)
    (COALESCE(jsonb_array_length(damage_relations->'double_damage_to'), 0) * 2) -
    (COALESCE(jsonb_array_length(damage_relations->'half_damage_to'), 0)) -
    (COALESCE(jsonb_array_length(damage_relations->'no_damage_to'), 0) * 2) AS offensive_score,
    
    (COALESCE(jsonb_array_length(damage_relations->'half_damage_from'), 0)) +
    (COALESCE(jsonb_array_length(damage_relations->'no_damage_from'), 0) * 2) -
    (COALESCE(jsonb_array_length(damage_relations->'double_damage_from'), 0)) AS defensive_score,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data