/*
  Model: dim_locations
  Description: Consolidated dimension table for locations across fictional universes
  
  Notes:
  - Combines Star Wars planets, Pokémon regions, and Netrunner locations
  - Standardizes physical attributes across universes
  - Adds derived classification fields for habitability and environment
  - Includes universe-specific attributes while maintaining consistent schema
*/

WITH sw_planets AS (
    SELECT
        'sw_' || id AS source_id,
        'star_wars' AS universe,
        planet_name AS location_name,  -- Updated to match improved staging model naming
        'Planet' AS location_type,
        diameter,
        rotation_period,
        orbital_period,
        gravity,
        population,
        climate,
        terrain,
        surface_water,
        
        -- Add new fields from enhanced staging model
        resident_count,
        film_appearances,
        
        -- Add classification flags for better filtering
        is_temperate,
        has_vegetation,
        is_water_world,
        is_desert_world,
        
        -- Add source reference information
        url AS source_url,
        
        -- Add name arrays for easier reporting
        resident_names,
        film_names AS featured_in_films,
        
        -- Add ETL tracking fields for lineage
        fetch_timestamp,
        processed_timestamp
    FROM {{ ref('stg_swapi_planets') }}
),

pokemon_regions AS (
    -- Extract distinct regions from Pokémon data
    SELECT DISTINCT
        -- Generate source_id from region name
        'pkm_' || {{ dbt_utils.generate_surrogate_key(['region']) }} AS source_id,
        'pokemon' AS universe,
        region AS location_name,
        'Region' AS location_type,
        NULL AS diameter,
        NULL AS rotation_period,
        NULL AS orbital_period,
        '1 standard' AS gravity,  -- Earth-like gravity
        CASE  -- Approximate population based on region
            WHEN region = 'Kanto' THEN 10000000
            WHEN region = 'Johto' THEN 9000000
            WHEN region = 'Hoenn' THEN 7500000
            WHEN region = 'Sinnoh' THEN 8500000
            WHEN region = 'Unova' THEN 12000000
            WHEN region = 'Kalos' THEN 11000000
            WHEN region = 'Alola' THEN 1000000
            WHEN region = 'Galar' THEN 15000000
            WHEN region = 'Paldea' THEN 14000000  -- Added new region
            ELSE 5000000
        END AS population,
        CASE  -- Climate based on region
            WHEN region = 'Kanto' THEN 'temperate'
            WHEN region = 'Johto' THEN 'temperate'
            WHEN region = 'Hoenn' THEN 'tropical'
            WHEN region = 'Sinnoh' THEN 'cold, temperate'
            WHEN region = 'Unova' THEN 'varied, metropolitan'
            WHEN region = 'Kalos' THEN 'temperate, varied'
            WHEN region = 'Alola' THEN 'tropical'
            WHEN region = 'Galar' THEN 'temperate, cold'
            WHEN region = 'Paldea' THEN 'mediterranean, varied'  -- Added new region
            ELSE 'varied'
        END AS climate,
        CASE  -- Terrain based on region
            WHEN region = 'Kanto' THEN 'mountains, forests, urban'
            WHEN region = 'Johto' THEN 'mountains, forests, rural'
            WHEN region = 'Hoenn' THEN 'islands, volcanoes, forests'
            WHEN region = 'Sinnoh' THEN 'mountains, lakes, snow'
            WHEN region = 'Unova' THEN 'urban, bridges, desert'
            WHEN region = 'Kalos' THEN 'mountains, urban, coastal'
            WHEN region = 'Alola' THEN 'islands, beaches, volcanoes'
            WHEN region = 'Galar' THEN 'countryside, industrial, hills'
            WHEN region = 'Paldea' THEN 'mountains, lakes, coastal, olive groves'  -- Added new region
            ELSE 'varied'
        END AS terrain,
        CASE  -- Surface water percentage based on region
            WHEN region = 'Hoenn' THEN 70
            WHEN region = 'Alola' THEN 80
            WHEN region = 'Sinnoh' THEN 40
            WHEN region = 'Kanto' THEN 30
            WHEN region = 'Johto' THEN 25
            WHEN region = 'Unova' THEN 35
            WHEN region = 'Kalos' THEN 30
            WHEN region = 'Galar' THEN 20
            WHEN region = 'Paldea' THEN 45  -- Added new region
            ELSE 30
        END AS surface_water
    FROM {{ ref('stg_pokeapi_pokemon') }}
    WHERE region IS NOT NULL
),

netrunner_locations AS (
    -- Create major Netrunner universe locations based on lore
    SELECT * FROM (VALUES
        ('nr_1001', 'netrunner', 'New Angeles', 'Megacity', NULL, NULL, NULL, 'high', 500000000, 'temperate, controlled', 'urban, corporate', 10),
        ('nr_1002', 'netrunner', 'The Moon', 'Colony', 3474, 27.3, 27.3, 'low', 5000000, 'artificial', 'lunar, domes', 0),
        ('nr_1003', 'netrunner', 'ChiLo', 'District', NULL, NULL, NULL, 'standard', 80000000, 'temperate', 'urban sprawl', 15),
        ('nr_1004', 'netrunner', 'Mars', 'Colony', 6779, 24.6, 687, 'low', 2000000, 'arid, cold', 'desert, domes', 0),
        ('nr_1005', 'netrunner', 'Heinlein', 'Orbital', NULL, NULL, NULL, 'artificial', 50000, 'artificial', 'station, corporate', 0),
        ('nr_1006', 'netrunner', 'Mumbad', 'Megacity', NULL, NULL, NULL, 'standard', 450000000, 'tropical, monsoon', 'urban, slums', 20),
        ('nr_1007', 'netrunner', 'Jinteki Biotech Labs', 'Facility', NULL, NULL, NULL, 'standard', 15000, 'controlled', 'research, corporate', 5)  -- Added new location
    ) AS v(source_id, universe, location_name, location_type, diameter, rotation_period, orbital_period, gravity, population, climate, terrain, surface_water)
),

-- Combine all locations
all_locations AS (
    SELECT * FROM sw_planets
    UNION ALL
    SELECT * FROM pokemon_regions
    UNION ALL
    SELECT * FROM netrunner_locations
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['universe', 'source_id']) }} AS location_key,
    source_id,
    universe,
    location_name,
    location_type,
    
    -- Physical attributes with proper NULL handling
    diameter,
    rotation_period,
    orbital_period,
    COALESCE(gravity, 'unknown') AS gravity,
    population,
    COALESCE(climate, 'unknown') AS climate,
    COALESCE(terrain, 'unknown') AS terrain,
    surface_water,
    
    -- Derived classifications
    CASE
        WHEN population IS NULL THEN 'Unknown'
        WHEN population = 0 THEN 'Uninhabited'
        WHEN population < 1000000 THEN 'Low Population'
        WHEN population < 100000000 THEN 'Medium Population'
        WHEN population < 1000000000 THEN 'High Population'
        ELSE 'Densely Populated'
    END AS population_class,
    
    -- Biome classification with improved NULL handling
    CASE
        WHEN climate IS NULL THEN 'Unknown'
        WHEN LOWER(climate) LIKE '%arid%' OR LOWER(climate) LIKE '%desert%' THEN 'Arid'
        WHEN LOWER(climate) LIKE '%tropical%' THEN 'Tropical'
        WHEN LOWER(climate) LIKE '%temperate%' THEN 'Temperate'
        WHEN LOWER(climate) LIKE '%cold%' OR LOWER(climate) LIKE '%frozen%' OR LOWER(climate) LIKE '%ice%' THEN 'Cold'
        WHEN LOWER(climate) LIKE '%artificial%' THEN 'Artificial'
        ELSE 'Mixed'
    END AS biome_class,
    
    -- Water feature classification with NULL handling
    CASE
        WHEN surface_water IS NULL THEN 'Unknown'
        WHEN surface_water = 0 THEN 'Arid'
        WHEN surface_water < 30 THEN 'Limited Waters'
        WHEN surface_water < 70 THEN 'Moderate Waters'
        ELSE 'Water-Rich'
    END AS water_class,
    
    -- Habitability score (0-100) with improved logic
    CASE
        WHEN universe = 'star_wars' THEN
            CASE
                WHEN is_temperate = TRUE THEN 85  -- Temperate planets are highly habitable
                WHEN has_vegetation = TRUE THEN 75  -- Vegetation indicates habitability
                WHEN is_water_world = TRUE THEN 65  -- Water worlds moderately habitable
                WHEN is_desert_world = TRUE THEN 40  -- Desert worlds less habitable
                WHEN climate IS NULL THEN 50  -- Default for unknown
                WHEN population = 0 OR LOWER(climate) LIKE '%frozen%' THEN 10
                WHEN LOWER(climate) LIKE '%temperate%' AND COALESCE(surface_water, 0) > 0 THEN 
                    LEAST(100, 50 + (COALESCE(surface_water, 0) / 2))
                ELSE 50
            END
        WHEN universe = 'pokemon' THEN
            CASE  -- Pokémon regions are generally habitable
                WHEN location_name IN ('Kanto', 'Johto', 'Kalos') THEN 95
                WHEN location_name = 'Alola' THEN 98  -- Paradise
                WHEN location_name = 'Sinnoh' THEN 85  -- Colder
                WHEN location_name = 'Paldea' THEN 92  -- Newer region
                ELSE 90
            END
        WHEN universe = 'netrunner' THEN
            CASE
                WHEN location_name IN ('Mars', 'The Moon', 'Heinlein') THEN 40  -- Artificial habitats
                WHEN location_name = 'New Angeles' THEN 70  -- Controlled but crowded
                WHEN location_name = 'Jinteki Biotech Labs' THEN 75  -- Controlled environment
                ELSE 60
            END
        ELSE 50
    END AS habitability_score,
    
    -- Notable lore locations flag
    CASE
        WHEN universe = 'star_wars' AND (
             location_name IN ('Tatooine', 'Coruscant', 'Hoth', 'Endor', 'Naboo', 'Alderaan', 'Dagobah', 'Mustafar', 'Kashyyyk')
             OR COALESCE(film_appearances, 0) >= 3  -- Appears in 3+ films
        ) THEN TRUE
        WHEN universe = 'pokemon' AND 
             location_name IN ('Kanto', 'Johto', 'Hoenn', 'Sinnoh', 'Unova', 'Kalos', 'Alola') THEN TRUE
        WHEN universe = 'netrunner' AND 
             location_name IN ('New Angeles', 'ChiLo', 'Heinlein') THEN TRUE
        ELSE FALSE
    END AS is_iconic_location,
    
    -- Add urbanization level
    CASE
        WHEN terrain IS NULL THEN 'Unknown'
        WHEN LOWER(terrain) LIKE '%urban%' OR LOWER(terrain) LIKE '%city%' OR LOWER(terrain) LIKE '%metropol%' THEN
            CASE
                WHEN population > 500000000 THEN 'Megalopolis'
                WHEN population > 100000000 THEN 'Major Urban'
                ELSE 'Urban'
            END
        WHEN LOWER(terrain) LIKE '%rural%' OR LOWER(terrain) LIKE '%village%' THEN 'Rural'
        WHEN LOWER(terrain) LIKE '%uninhabited%' OR population = 0 THEN 'Uninhabited'
        ELSE 'Mixed/Wilderness'
    END AS urbanization_level,
    
    -- Add new cross-universe metrics
    CASE
        WHEN universe = 'star_wars' THEN COALESCE(resident_count, 0)
        WHEN universe = 'pokemon' THEN NULL -- Could add if you have Pokémon character count per region
        WHEN universe = 'netrunner' THEN NULL -- Could add if you have character count per location
        ELSE 0
    END AS character_count,
    
    CASE
        WHEN universe = 'star_wars' THEN COALESCE(film_appearances, 0)
        WHEN universe = 'pokemon' THEN NULL -- Could map games to regions
        WHEN universe = 'netrunner' THEN NULL -- Could map card cycles to locations
        ELSE 0
    END AS media_appearances,
    
    -- Add featured in list (for Star Wars)
    CASE
        WHEN universe = 'star_wars' THEN featured_in_films
        ELSE NULL
    END AS featured_in,
    
    -- Add source tracking
    CASE
        WHEN universe = 'star_wars' THEN source_url
        ELSE NULL
    END AS source_url,
    
    -- Add ETL tracking fields for lineage
    CASE
        WHEN universe = 'star_wars' THEN fetch_timestamp
        ELSE NULL
    END AS data_fetch_timestamp,
    
    -- Add data tracking fields
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM all_locations
ORDER BY universe, location_name