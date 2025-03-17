/*
  Model: stg_pokeapi_pokemon
  Description: Standardizes Pokémon data from the PokeAPI
  Source: raw.pokeapi_pokemon
  
  Notes:
  - Type information is extracted from nested JSON structure
  - Region is derived from Pokémon ID ranges where not directly available
  - Stats are extracted from the nested stats array
  - Physical attributes are converted to standard units (kg, dm)
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        name,
        height,
        weight,
        base_experience,
        types,
        stats,
        abilities,
        moves,
        region,  -- Assuming this field exists in your enriched data
        is_legendary,
        is_mythical,
        species,
        generation
    FROM raw.pokeapi_pokemon
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    name,
    
    -- Extract primary type (first type in the array)
    CASE 
        WHEN types IS NULL OR jsonb_array_length(types) = 0 THEN 'Unknown'
        ELSE COALESCE(types->0->>'type'->>'name', 'Unknown')
    END AS primary_type,
    
    -- Extract all types as an array with error handling
    CASE
        WHEN types IS NULL THEN ARRAY['Unknown']::VARCHAR[]
        ELSE ARRAY(
            SELECT COALESCE(jsonb_array_elements(types)->>'type'->>'name', 'Unknown')
            WHERE jsonb_typeof(types) = 'array'
        )
    END AS types,
    
    -- Extract region with more comprehensive generation mapping
    COALESCE(region, 
             CASE 
                 WHEN id <= 151 THEN 'Kanto'      -- Gen 1
                 WHEN id <= 251 THEN 'Johto'      -- Gen 2
                 WHEN id <= 386 THEN 'Hoenn'      -- Gen 3
                 WHEN id <= 493 THEN 'Sinnoh'     -- Gen 4
                 WHEN id <= 649 THEN 'Unova'      -- Gen 5
                 WHEN id <= 721 THEN 'Kalos'      -- Gen 6
                 WHEN id <= 809 THEN 'Alola'      -- Gen 7
                 WHEN id <= 898 THEN 'Galar'      -- Gen 8
                 WHEN id <= 1008 THEN 'Paldea'    -- Gen 9
                 ELSE 'Unknown'
             END) AS region,
    
    -- Extract generation number for easier filtering
    CASE
        WHEN id <= 151 THEN 1
        WHEN id <= 251 THEN 2
        WHEN id <= 386 THEN 3
        WHEN id <= 493 THEN 4
        WHEN id <= 649 THEN 5
        WHEN id <= 721 THEN 6
        WHEN id <= 809 THEN 7
        WHEN id <= 898 THEN 8
        WHEN id <= 1008 THEN 9
        ELSE NULL
    END AS generation_number,
    
    -- Physical attributes with unit conversions
    COALESCE(CAST(height AS NUMERIC), 0) AS height,           -- In decimeters
    COALESCE(CAST(weight AS NUMERIC) / 10, 0) AS weight,      -- Convert to kg
    
    -- Calculate BMI (weight in kg / height in meters squared)
    CASE
        WHEN COALESCE(height, 0) > 0 
        THEN ROUND((COALESCE(weight, 0) * 10) / POWER(height, 2), 1)
        ELSE NULL
    END AS bmi,
    
    -- Experience and rarity
    COALESCE(base_experience, 0) AS base_xp,
    COALESCE(is_legendary, FALSE) AS is_legendary,
    COALESCE(is_mythical, FALSE) AS is_mythical,
    
    -- Base stats with proper null handling
    COALESCE((SELECT (value->>'base_stat')::integer 
              FROM jsonb_array_elements(stats) 
              WHERE value->>'stat'->>'name' = 'hp'
              LIMIT 1), 0) AS base_stat_hp,
    
    COALESCE((SELECT (value->>'base_stat')::integer 
              FROM jsonb_array_elements(stats) 
              WHERE value->>'stat'->>'name' = 'attack'
              LIMIT 1), 0) AS base_stat_attack,
    
    COALESCE((SELECT (value->>'base_stat')::integer 
              FROM jsonb_array_elements(stats) 
              WHERE value->>'stat'->>'name' = 'defense'
              LIMIT 1), 0) AS base_stat_defense,
    
    COALESCE((SELECT (value->>'base_stat')::integer 
              FROM jsonb_array_elements(stats) 
              WHERE value->>'stat'->>'name' = 'special-attack'
              LIMIT 1), 0) AS base_stat_special_attack,
    
    COALESCE((SELECT (value->>'base_stat')::integer 
              FROM jsonb_array_elements(stats) 
              WHERE value->>'stat'->>'name' = 'special-defense'
              LIMIT 1), 0) AS base_stat_special_defense,
    
    COALESCE((SELECT (value->>'base_stat')::integer 
              FROM jsonb_array_elements(stats) 
              WHERE value->>'stat'->>'name' = 'speed'
              LIMIT 1), 0) AS base_stat_speed,
    
    -- Total base stats - useful for power comparisons
    (
        COALESCE((SELECT (value->>'base_stat')::integer FROM jsonb_array_elements(stats) WHERE value->>'stat'->>'name' = 'hp' LIMIT 1), 0) +
        COALESCE((SELECT (value->>'base_stat')::integer FROM jsonb_array_elements(stats) WHERE value->>'stat'->>'name' = 'attack' LIMIT 1), 0) +
        COALESCE((SELECT (value->>'base_stat')::integer FROM jsonb_array_elements(stats) WHERE value->>'stat'->>'name' = 'defense' LIMIT 1), 0) +
        COALESCE((SELECT (value->>'base_stat')::integer FROM jsonb_array_elements(stats) WHERE value->>'stat'->>'name' = 'special-attack' LIMIT 1), 0) +
        COALESCE((SELECT (value->>'base_stat')::integer FROM jsonb_array_elements(stats) WHERE value->>'stat'->>'name' = 'special-defense' LIMIT 1), 0) +
        COALESCE((SELECT (value->>'base_stat')::integer FROM jsonb_array_elements(stats) WHERE value->>'stat'->>'name' = 'speed' LIMIT 1), 0)
    ) AS total_base_stats,
    
    -- Extract abilities as structured data
    abilities,
    
    -- Count the number of abilities and moves
    COALESCE(jsonb_array_length(abilities), 0) AS ability_count,
    COALESCE(jsonb_array_length(moves), 0) AS move_count,
    
    -- Starter Pokémon flag
    CASE
        WHEN name IN ('bulbasaur', 'charmander', 'squirtle', 
                      'chikorita', 'cyndaquil', 'totodile',
                      'treecko', 'torchic', 'mudkip',
                      'turtwig', 'chimchar', 'piplup',
                      'snivy', 'tepig', 'oshawott',
                      'chespin', 'fennekin', 'froakie',
                      'rowlet', 'litten', 'popplio',
                      'grookey', 'scorbunny', 'sobble') THEN TRUE
        ELSE FALSE
    END AS is_starter,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data