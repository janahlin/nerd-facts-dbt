

/*
  Model: dim_pokemon_types
  Description: Dimension table for Pokémon types and their effectiveness relationships
  
  Notes:
  - Contains comprehensive type effectiveness data (weaknesses, resistances, immunities)
  - Includes type distribution metrics across the Pokédex
  - Provides visual attributes for UI presentation
  - Calculates offensive and defensive ratings
  - Adds type categorization and generation data
*/

WITH type_counts AS (
    -- Get primary type usage
    SELECT
        COALESCE(primary_type, 'Unknown') AS type_name,
        COUNT(*) AS num_primary
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    GROUP BY primary_type
),

secondary_type_counts AS (
    -- Get secondary type usage from type_list array
    SELECT 
        COALESCE(type_list[1], 'None') AS type_name,  -- Changed from secondary_type
        COUNT(*) AS num_secondary
    FROM "nerd_facts"."public"."stg_pokeapi_pokemon"
    WHERE type_list[1] IS NOT NULL
    GROUP BY type_list[1]
),

combined_counts AS (
    -- Combine primary and secondary counts
    SELECT
        t.type_name,
        t.num_primary,
        COALESCE(s.num_secondary, 0) AS num_secondary,
        t.num_primary + COALESCE(s.num_secondary, 0) AS total_usage
    FROM type_counts t
    LEFT JOIN secondary_type_counts s ON t.type_name = s.type_name
),

type_attributes AS (
    SELECT
        type_name,
        -- Weaknesses (takes 2x damage from these types)
        CASE
            WHEN type_name = 'Normal' THEN ARRAY['Fighting']
            WHEN type_name = 'Fire' THEN ARRAY['Water', 'Ground', 'Rock']
            WHEN type_name = 'Water' THEN ARRAY['Electric', 'Grass']
            WHEN type_name = 'Electric' THEN ARRAY['Ground']
            WHEN type_name = 'Grass' THEN ARRAY['Fire', 'Ice', 'Poison', 'Flying', 'Bug']
            WHEN type_name = 'Ice' THEN ARRAY['Fire', 'Fighting', 'Rock', 'Steel']
            WHEN type_name = 'Fighting' THEN ARRAY['Flying', 'Psychic', 'Fairy']
            WHEN type_name = 'Poison' THEN ARRAY['Ground', 'Psychic']
            WHEN type_name = 'Ground' THEN ARRAY['Water', 'Grass', 'Ice']
            WHEN type_name = 'Flying' THEN ARRAY['Electric', 'Ice', 'Rock']
            WHEN type_name = 'Psychic' THEN ARRAY['Bug', 'Ghost', 'Dark']
            WHEN type_name = 'Bug' THEN ARRAY['Fire', 'Flying', 'Rock']
            WHEN type_name = 'Rock' THEN ARRAY['Water', 'Grass', 'Fighting', 'Ground', 'Steel']
            WHEN type_name = 'Ghost' THEN ARRAY['Ghost', 'Dark']
            WHEN type_name = 'Dragon' THEN ARRAY['Ice', 'Dragon', 'Fairy']
            WHEN type_name = 'Dark' THEN ARRAY['Fighting', 'Bug', 'Fairy']
            WHEN type_name = 'Steel' THEN ARRAY['Fire', 'Fighting', 'Ground']
            WHEN type_name = 'Fairy' THEN ARRAY['Poison', 'Steel']
            ELSE ARRAY[]::VARCHAR[]
        END AS weaknesses,
        
        -- Resistances (takes 0.5x damage from these types)
        CASE
            WHEN type_name = 'Normal' THEN ARRAY[]::VARCHAR[]
            WHEN type_name = 'Fire' THEN ARRAY['Fire', 'Grass', 'Ice', 'Bug', 'Steel', 'Fairy']
            WHEN type_name = 'Water' THEN ARRAY['Fire', 'Water', 'Ice', 'Steel']
            WHEN type_name = 'Electric' THEN ARRAY['Electric', 'Flying', 'Steel']
            WHEN type_name = 'Grass' THEN ARRAY['Water', 'Electric', 'Grass', 'Ground']
            WHEN type_name = 'Ice' THEN ARRAY['Ice']
            WHEN type_name = 'Fighting' THEN ARRAY['Bug', 'Rock', 'Dark']
            WHEN type_name = 'Poison' THEN ARRAY['Grass', 'Fighting', 'Poison', 'Bug', 'Fairy']
            WHEN type_name = 'Ground' THEN ARRAY['Poison', 'Rock']
            WHEN type_name = 'Flying' THEN ARRAY['Grass', 'Fighting', 'Bug']
            WHEN type_name = 'Psychic' THEN ARRAY['Fighting', 'Psychic']
            WHEN type_name = 'Bug' THEN ARRAY['Grass', 'Fighting', 'Ground']
            WHEN type_name = 'Rock' THEN ARRAY['Normal', 'Fire', 'Poison', 'Flying']
            WHEN type_name = 'Ghost' THEN ARRAY['Poison', 'Bug']  -- Fixed: Ghost resists Bug and Poison
            WHEN type_name = 'Dragon' THEN ARRAY['Fire', 'Water', 'Electric', 'Grass']
            WHEN type_name = 'Dark' THEN ARRAY['Ghost', 'Dark']
            WHEN type_name = 'Steel' THEN ARRAY['Normal', 'Grass', 'Ice', 'Flying', 'Psychic', 'Bug', 'Rock', 'Dragon', 'Steel', 'Fairy']
            WHEN type_name = 'Fairy' THEN ARRAY['Fighting', 'Bug', 'Dark']
            ELSE ARRAY[]::VARCHAR[]
        END AS resistances,
        
        -- Immunities (takes 0x damage from these types)
        CASE
            WHEN type_name = 'Normal' THEN ARRAY['Ghost']
            WHEN type_name = 'Flying' THEN ARRAY['Ground']
            WHEN type_name = 'Ground' THEN ARRAY['Electric']
            WHEN type_name = 'Ghost' THEN ARRAY['Normal', 'Fighting']
            WHEN type_name = 'Dark' THEN ARRAY['Psychic']
            WHEN type_name = 'Fairy' THEN ARRAY['Dragon']
            WHEN type_name = 'Steel' THEN ARRAY['Poison']
            ELSE ARRAY[]::VARCHAR[]
        END AS immunities,
        
        -- Super effective against (deals 2x damage to these types)
        CASE
            WHEN type_name = 'Normal' THEN ARRAY[]::VARCHAR[]
            WHEN type_name = 'Fire' THEN ARRAY['Grass', 'Ice', 'Bug', 'Steel']
            WHEN type_name = 'Water' THEN ARRAY['Fire', 'Ground', 'Rock']
            WHEN type_name = 'Electric' THEN ARRAY['Water', 'Flying']
            WHEN type_name = 'Grass' THEN ARRAY['Water', 'Ground', 'Rock']
            WHEN type_name = 'Ice' THEN ARRAY['Grass', 'Ground', 'Flying', 'Dragon']
            WHEN type_name = 'Fighting' THEN ARRAY['Normal', 'Ice', 'Rock', 'Dark', 'Steel']
            WHEN type_name = 'Poison' THEN ARRAY['Grass', 'Fairy']
            WHEN type_name = 'Ground' THEN ARRAY['Fire', 'Electric', 'Poison', 'Rock', 'Steel']
            WHEN type_name = 'Flying' THEN ARRAY['Grass', 'Fighting', 'Bug']
            WHEN type_name = 'Psychic' THEN ARRAY['Fighting', 'Poison']
            WHEN type_name = 'Bug' THEN ARRAY['Grass', 'Psychic', 'Dark']
            WHEN type_name = 'Rock' THEN ARRAY['Fire', 'Ice', 'Flying', 'Bug']
            WHEN type_name = 'Ghost' THEN ARRAY['Psychic', 'Ghost']
            WHEN type_name = 'Dragon' THEN ARRAY['Dragon']
            WHEN type_name = 'Dark' THEN ARRAY['Psychic', 'Ghost']
            WHEN type_name = 'Steel' THEN ARRAY['Ice', 'Rock', 'Fairy']
            WHEN type_name = 'Fairy' THEN ARRAY['Fighting', 'Dragon', 'Dark']
            ELSE ARRAY[]::VARCHAR[]
        END AS super_effective,
        
        -- Type compatibility (works well defensively with these types)
        CASE
            WHEN type_name = 'Normal' THEN ARRAY['Ghost', 'Rock', 'Steel']
            WHEN type_name = 'Fire' THEN ARRAY['Water', 'Rock', 'Dragon']
            WHEN type_name = 'Water' THEN ARRAY['Fire', 'Ground', 'Flying']
            WHEN type_name = 'Electric' THEN ARRAY['Flying', 'Steel', 'Fairy']
            WHEN type_name = 'Grass' THEN ARRAY['Poison', 'Flying', 'Bug']
            WHEN type_name = 'Ice' THEN ARRAY['Steel', 'Fire', 'Water']
            WHEN type_name = 'Fighting' THEN ARRAY['Flying', 'Psychic', 'Fairy']
            WHEN type_name = 'Poison' THEN ARRAY['Ground', 'Ghost', 'Steel']
            WHEN type_name = 'Ground' THEN ARRAY['Flying', 'Bug', 'Grass']
            WHEN type_name = 'Flying' THEN ARRAY['Steel', 'Electric', 'Rock']
            WHEN type_name = 'Psychic' THEN ARRAY['Steel', 'Dark', 'Normal']
            WHEN type_name = 'Bug' THEN ARRAY['Flying', 'Steel', 'Fire']
            WHEN type_name = 'Rock' THEN ARRAY['Fighting', 'Ground', 'Steel']
            WHEN type_name = 'Ghost' THEN ARRAY['Dark', 'Normal', 'Poison']
            WHEN type_name = 'Dragon' THEN ARRAY['Steel', 'Fairy']
            WHEN type_name = 'Dark' THEN ARRAY['Fighting', 'Fairy', 'Bug']
            WHEN type_name = 'Steel' THEN ARRAY['Fire', 'Electric', 'Water']
            WHEN type_name = 'Fairy' THEN ARRAY['Poison', 'Steel', 'Fire']
            ELSE ARRAY[]::VARCHAR[]
        END AS defensive_synergy
    FROM combined_counts
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(c.type_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS type_key,
    ROW_NUMBER() OVER (ORDER BY c.type_name) AS type_id,
    
    -- Core attributes
    c.type_name,
    c.num_primary,
    c.num_secondary,
    c.total_usage,
    
    -- Type effectiveness data
    a.weaknesses,
    a.resistances,
    a.immunities,
    a.super_effective,
    
    -- Defensive synergy types
    a.defensive_synergy,
    
    -- Calculated metrics
    COALESCE(ARRAY_LENGTH(a.weaknesses, 1), 0) AS weakness_count,
    COALESCE(ARRAY_LENGTH(a.resistances, 1), 0) AS resistance_count,
    COALESCE(ARRAY_LENGTH(a.immunities, 1), 0) AS immunity_count,
    COALESCE(ARRAY_LENGTH(a.super_effective, 1), 0) AS super_effective_count,
    
    -- Type color (for UI)
    CASE
        WHEN c.type_name = 'Normal' THEN '#A8A77A'
        WHEN c.type_name = 'Fire' THEN '#EE8130'
        WHEN c.type_name = 'Water' THEN '#6390F0'
        WHEN c.type_name = 'Electric' THEN '#F7D02C'
        WHEN c.type_name = 'Grass' THEN '#7AC74C'
        WHEN c.type_name = 'Ice' THEN '#96D9D6'
        WHEN c.type_name = 'Fighting' THEN '#C22E28'
        WHEN c.type_name = 'Poison' THEN '#A33EA1'
        WHEN c.type_name = 'Ground' THEN '#E2BF65'
        WHEN c.type_name = 'Flying' THEN '#A98FF3'
        WHEN c.type_name = 'Psychic' THEN '#F95587'
        WHEN c.type_name = 'Bug' THEN '#A6B91A'
        WHEN c.type_name = 'Rock' THEN '#B6A136'
        WHEN c.type_name = 'Ghost' THEN '#735797'
        WHEN c.type_name = 'Dragon' THEN '#6F35FC'
        WHEN c.type_name = 'Dark' THEN '#705746'
        WHEN c.type_name = 'Steel' THEN '#B7B7CE'
        WHEN c.type_name = 'Fairy' THEN '#D685AD'
        ELSE '#CCCCCC'
    END AS type_color,
    
    -- Type category - expanded with better organization
    CASE
        WHEN c.type_name IN ('Fire', 'Water', 'Electric') THEN 'Primary Elemental'
        WHEN c.type_name IN ('Grass', 'Ice') THEN 'Secondary Elemental'
        WHEN c.type_name IN ('Fighting', 'Rock', 'Ground') THEN 'Physical'
        WHEN c.type_name IN ('Poison', 'Flying', 'Bug') THEN 'Biological'
        WHEN c.type_name IN ('Psychic', 'Ghost', 'Dark', 'Fairy') THEN 'Special'
        WHEN c.type_name IN ('Normal') THEN 'Normal'
        WHEN c.type_name IN ('Dragon', 'Steel') THEN 'Advanced'
        ELSE 'Other'
    END AS type_category,
    
    -- Type attack classification
    CASE
        WHEN c.type_name IN ('Fire', 'Water', 'Electric', 'Grass', 'Ice', 
                           'Psychic', 'Dragon', 'Dark', 'Fairy') THEN 'Special'
        ELSE 'Physical'
    END AS damage_category,
    
    -- Generation introduced
    CASE
        WHEN c.type_name IN ('Normal', 'Fire', 'Water', 'Electric', 'Grass', 'Ice', 
                            'Fighting', 'Poison', 'Ground', 'Flying', 'Psychic', 'Bug', 
                            'Rock', 'Ghost', 'Dragon') THEN 1
        WHEN c.type_name IN ('Dark', 'Steel') THEN 2
        WHEN c.type_name IN ('Fairy') THEN 6
        ELSE 1
    END AS generation_introduced,
    
    -- Offensive usefulness rating (1-10)
    CASE
        WHEN c.type_name IN ('Ground', 'Fighting', 'Fire') THEN 9  -- Excellent coverage
        WHEN c.type_name IN ('Ice', 'Electric', 'Rock', 'Flying', 'Fairy', 'Ghost', 'Dark') THEN 8  -- Great coverage
        WHEN c.type_name IN ('Grass', 'Dragon', 'Steel', 'Water', 'Psychic') THEN 7  -- Good coverage
        WHEN c.type_name IN ('Poison', 'Bug') THEN 5  -- Limited coverage
        WHEN c.type_name IN ('Normal') THEN 3  -- Poor coverage
        ELSE 6
    END AS offensive_rating,
    
    -- Defensive usefulness rating (1-10)
    CASE
        WHEN c.type_name IN ('Steel', 'Fairy') THEN 10  -- Excellent defenses
        WHEN c.type_name IN ('Ghost', 'Water', 'Normal', 'Dragon') THEN 8  -- Great defenses
        WHEN c.type_name IN ('Fire', 'Flying', 'Dark', 'Poison', 'Ground') THEN 7  -- Good defenses
        WHEN c.type_name IN ('Electric', 'Fighting', 'Psychic', 'Rock') THEN 5  -- Average defenses
        WHEN c.type_name IN ('Grass', 'Bug', 'Ice') THEN 3  -- Poor defenses
        ELSE 6
    END AS defensive_rating,
    
    -- Type value calculated across multiple factors
    (
        COALESCE(ARRAY_LENGTH(a.resistances, 1), 0) * 5 +
        COALESCE(ARRAY_LENGTH(a.immunities, 1), 0) * 10 -
        COALESCE(ARRAY_LENGTH(a.weaknesses, 1), 0) * 5 +
        COALESCE(ARRAY_LENGTH(a.super_effective, 1), 0) * 3
    ) AS composite_value_score,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM combined_counts c
JOIN type_attributes a ON c.type_name = a.type_name
ORDER BY c.type_name