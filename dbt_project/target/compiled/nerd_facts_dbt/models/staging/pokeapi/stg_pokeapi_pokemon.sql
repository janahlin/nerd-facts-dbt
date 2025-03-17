/*
  Model: stg_pokeapi_pokemon
  Description: Standardizes Pokémon data from the PokeAPI
  Source: raw.pokeapi_pokemon
  
  Notes:
  - Type information is extracted from nested JSON structure
  - Region and Generation are derived from Pokémon ID ranges (not directly available)
  - Legendary status is derived from specific Pokémon IDs
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
        types::JSONB AS types,
        stats::JSONB AS stats,
        abilities::JSONB AS abilities,  -- Cast to JSONB
        moves::JSONB AS moves,
        species::JSONB AS species
        -- Removed generation column as it doesn't exist
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
        ELSE (types->0->'type'->>'name')
    END AS primary_type,
    
    -- Extract all types as an array with error handling
    ARRAY(
        SELECT type_obj->'type'->>'name'
        FROM jsonb_array_elements(COALESCE(types, '[]'::JSONB)) AS type_obj
    ) AS type_list,
    
    -- Derive region from ID ranges (since region column doesn't exist)
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
    END AS region,
    
    -- Extract generation number for easier filtering - derived from ID
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
    CASE 
        WHEN height::TEXT ~ '^[0-9]+(\.[0-9]+)?$' 
        THEN CAST(height AS NUMERIC) 
        ELSE 0 
    END AS height_dm,
    
    CASE 
        WHEN weight::TEXT ~ '^[0-9]+(\.[0-9]+)?$' 
        THEN CAST(weight AS NUMERIC) / 10
        ELSE 0 
    END AS weight_kg,
    
    -- Calculate BMI (weight in kg / height in meters squared)
    CASE
        WHEN height::TEXT ~ '^[0-9]+(\.[0-9]+)?$' AND CAST(height AS NUMERIC) > 0 
        THEN ROUND((CAST(weight AS NUMERIC) / 10) / POWER(CAST(height AS NUMERIC)/10, 2), 1)
        ELSE NULL
    END AS bmi,
    
    -- Experience and rarity
    CASE 
        WHEN base_experience::TEXT ~ '^[0-9]+(\.[0-9]+)?$' 
        THEN CAST(base_experience AS INTEGER)
        ELSE 0 
    END AS base_xp,
    
    -- Derived legendary status from known legendary Pokémon IDs
    CASE
        WHEN id IN (144, 145, 146, 150, 151, 243, 244, 245, 249, 250, 251,
                    377, 378, 379, 380, 381, 382, 383, 384, 385, 386,
                    480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493,
                    638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649,
                    716, 717, 718, 719, 720, 721) 
        THEN TRUE
        ELSE FALSE
    END AS is_legendary,
    
    -- Derived mythical status from known mythical Pokémon IDs
    CASE
        WHEN id IN (151, 251, 385, 386, 489, 490, 491, 492, 493,
                    647, 648, 649, 719, 720, 721) 
        THEN TRUE
        ELSE FALSE
    END AS is_mythical,
    
    -- Fixed base stats extraction with proper JSON navigation
    COALESCE((
        SELECT (stat->>'base_stat')::INTEGER
        FROM jsonb_array_elements(stats) AS stat
        WHERE stat->'stat'->>'name' = 'hp'
        LIMIT 1
    ), 0) AS base_stat_hp,
    
    -- Fixed total base stats calculation
    COALESCE((
        SELECT SUM((stat->>'base_stat')::INTEGER)
        FROM jsonb_array_elements(stats) AS stat
    ), 0) AS total_base_stats,
    
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
    
    -- Extract abilities as a JSONB array
    abilities AS ability_list,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data