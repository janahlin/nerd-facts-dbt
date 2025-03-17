

/*
  Model: dim_sw_planets
  Description: Dimension table for Star Wars planets and celestial bodies
  
  Notes:
  - Contains all planets from the Star Wars universe in SWAPI
  - Provides physical characteristics and environmental classifications
  - Calculates habitability metrics and significance rankings
  - Adds contextual information about each planet's role in the saga
  - Includes galactic region mapping and political affiliations
  - Enhanced with additional fields from updated staging models
*/

WITH planets AS (
    SELECT
        id AS planet_id,
        planet_name,
        rotation_period,
        orbital_period,
        diameter,
        climate,
        gravity,
        terrain,
        surface_water,
        population,
        
        -- Add new fields from enhanced staging model
        resident_count,
        film_appearances,
        film_names,
        resident_names,
        
        -- Add terrain classifications from staging
        is_temperate,
        has_vegetation,
        is_water_world,
        is_desert_world,
        
        -- Add source tracking
        url,
        fetch_timestamp,
        processed_timestamp
    FROM "nerd_facts"."public"."stg_swapi_planets"
),

-- Add derived classification attributes with enhanced logic
planet_attributes AS (
    SELECT 
        *,
        -- Planet size classification with better ranges
        CASE
            WHEN diameter IS NULL THEN 'Unknown'
            WHEN diameter <= 5000 THEN 'Tiny'
            WHEN diameter <= 10000 THEN 'Small'
            WHEN diameter <= 15000 THEN 'Medium'
            WHEN diameter <= 25000 THEN 'Large'
            ELSE 'Massive'
        END AS size_class,
        
        -- Enhanced climate classification with better pattern matching
        CASE
            WHEN climate IS NULL THEN 'Unknown'
            WHEN climate LIKE '%temperate%' AND climate LIKE '%tropical%' THEN 'Temperate/Tropical Mix'
            WHEN climate LIKE '%temperate%' OR is_temperate = TRUE THEN 'Temperate'
            WHEN climate LIKE '%tropical%' THEN 'Tropical'
            WHEN climate LIKE '%arid%' OR climate LIKE '%hot%' OR climate LIKE '%desert%' 
                 OR is_desert_world = TRUE THEN 'Hot/Arid'
            WHEN climate LIKE '%frozen%' OR climate LIKE '%frigid%' OR climate LIKE '%cold%' 
                 OR climate LIKE '%ice%' THEN 'Cold/Frozen'
            WHEN climate LIKE '%polluted%' OR climate LIKE '%toxic%' THEN 'Polluted/Toxic'
            WHEN climate LIKE '%artificial%' OR climate LIKE '%controlled%' THEN 'Artificial/Controlled'
            WHEN climate LIKE '%moist%' OR climate LIKE '%humid%' OR climate LIKE '%wet%' 
                 OR is_water_world = TRUE THEN 'Humid/Moist'
            WHEN climate LIKE '%superheated%' OR climate LIKE '%fiery%' OR climate LIKE '%volcanic%' THEN 'Extreme Heat'
            ELSE 'Mixed/Other'
        END AS climate_class,
        
        -- Terrain type classification (primary terrain type)
        CASE
            WHEN terrain IS NULL THEN 'Unknown'
            WHEN terrain LIKE '%desert%' OR is_desert_world = TRUE THEN 'Desert'
            WHEN terrain LIKE '%forest%' AND terrain LIKE '%jungle%' THEN 'Forest/Jungle'
            WHEN terrain LIKE '%forest%' OR has_vegetation = TRUE THEN 'Forest'
            WHEN terrain LIKE '%jungle%' THEN 'Jungle'
            WHEN terrain LIKE '%mountain%' THEN 'Mountainous'
            WHEN terrain LIKE '%ocean%' OR terrain LIKE '%water%' OR terrain LIKE '%lake%' 
                 OR is_water_world = TRUE THEN 'Oceanic'
            WHEN terrain LIKE '%swamp%' OR terrain LIKE '%bog%' THEN 'Swamp/Bog'
            WHEN terrain LIKE '%urban%' OR terrain LIKE '%cityscape%' OR terrain LIKE '%city%' THEN 'Urban'
            WHEN terrain LIKE '%grass%' OR terrain LIKE '%plain%' OR terrain LIKE '%prairie%' THEN 'Grassland'
            WHEN terrain LIKE '%rock%' OR terrain LIKE '%cliff%' OR terrain LIKE '%canyon%' THEN 'Rocky'
            WHEN terrain LIKE '%ice%' OR terrain LIKE '%glacier%' OR terrain LIKE '%frozen%' THEN 'Ice/Frozen'
            WHEN terrain LIKE '%gas%' THEN 'Gas Giant'
            ELSE 'Mixed/Other'
        END AS terrain_class,
        
        -- Improved habitability score (0-100) with more factors
        CASE
            WHEN population IS NULL AND climate IS NULL THEN 50
            ELSE
                LEAST(100, GREATEST(0, 
                    -- Base score
                    40 + 
                    -- Climate factors
                    CASE 
                        WHEN is_temperate = TRUE THEN 20
                        WHEN climate LIKE '%temperate%' THEN 20 
                        WHEN climate LIKE '%tropical%' THEN 15
                        WHEN climate LIKE '%polluted%' OR climate LIKE '%toxic%' THEN -30
                        WHEN climate LIKE '%frigid%' OR climate LIKE '%frozen%' THEN -20
                        WHEN climate LIKE '%arid%' OR climate LIKE '%desert%' OR is_desert_world = TRUE THEN -15
                        ELSE 0 
                    END +
                    -- Water factors (crucial for life)
                    CASE 
                        WHEN is_water_world = TRUE THEN 15
                        WHEN surface_water > 50 THEN 15
                        WHEN surface_water > 30 THEN 10
                        WHEN surface_water > 10 THEN 5
                        WHEN surface_water = 0 THEN -10
                        WHEN surface_water IS NULL THEN 0
                        ELSE 0 
                    END +
                    -- Vegetation factors
                    CASE
                        WHEN has_vegetation = TRUE THEN 10
                        ELSE 0
                    END +
                    -- Population factors (evidence of habitability)
                    CASE 
                        WHEN population > 1000000000 THEN 25
                        WHEN population > 100000000 THEN 20
                        WHEN population > 10000000 THEN 15
                        WHEN population > 1000000 THEN 10
                        WHEN population > 0 THEN 5
                        WHEN population = 0 THEN -10
                        WHEN population IS NULL THEN 0
                        ELSE 0 
                    END +
                    -- Gravity factors
                    CASE 
                        WHEN gravity LIKE '%standard%' THEN 10
                        WHEN gravity LIKE '%high%' OR gravity LIKE '%heavy%' THEN -5
                        ELSE 0 
                    END
                ))
        END AS habitability_score,
        
        -- Planet's galactic region based on known planets
        CASE
            WHEN planet_name IN ('Coruscant', 'Alderaan', 'Corellia', 'Chandrila', 'Hosnian Prime') THEN 'Core Worlds'
            WHEN planet_name IN ('Kashyyyk', 'Duro', 'Abregado-rae', 'Cato Neimoidia', 'Fondor') THEN 'Colonies/Inner Rim'
            WHEN planet_name IN ('Naboo', 'Bothawui', 'Mon Cala', 'Onderon', 'Malastare') THEN 'Mid Rim'
            WHEN planet_name IN ('Tatooine', 'Geonosis', 'Ryloth', 'Dantooine', 'Lothal', 'Kessel') THEN 'Outer Rim'
            WHEN planet_name IN ('Kamino', 'Mustafar', 'Hoth', 'Bespin', 'Dagobah', 'Endor', 'Yavin IV') THEN 'Outer Rim/Unknown Regions'
            WHEN planet_name IN ('Exegol', 'Ilum', 'Csilla', 'Rakata Prime') THEN 'Unknown Regions'
            ELSE 'Unspecified Region'
        END AS galactic_region
    FROM planets
),

-- Additional planet significance data
planet_significance AS (
    SELECT 
        *,
        -- Major battles/events on planet
        CASE
            WHEN planet_name = 'Naboo' THEN 'Trade Federation Invasion, Battle of Naboo'
            WHEN planet_name = 'Geonosis' THEN 'First Battle of Geonosis, Start of Clone Wars'
            WHEN planet_name = 'Coruscant' THEN 'Senate location, Battle of Coruscant, Order 66'
            WHEN planet_name = 'Mustafar' THEN 'Anakin vs Obi-Wan duel, Sith stronghold'
            WHEN planet_name = 'Utapau' THEN 'Battle of Utapau, General Grievous death'
            WHEN planet_name = 'Kashyyyk' THEN 'Battle of Kashyyyk, Order 66'
            WHEN planet_name = 'Tatooine' THEN 'Skywalker homeworld, Jabba Palace, Podracing'
            WHEN planet_name = 'Alderaan' THEN 'Death Star destruction'
            WHEN planet_name = 'Yavin IV' THEN 'Battle of Yavin, Death Star destruction'
            WHEN planet_name = 'Hoth' THEN 'Battle of Hoth, Echo Base'
            WHEN planet_name = 'Dagobah' THEN 'Yoda exile, Luke training'
            WHEN planet_name = 'Bespin' THEN 'Cloud City, "I am your father" revelation'
            WHEN planet_name = 'Endor' THEN 'Battle of Endor, Death Star II destruction'
            WHEN planet_name = 'Jakku' THEN 'Battle of Jakku, Empire defeat, Rey homeworld'
            WHEN planet_name = 'Starkiller Base' THEN 'Hosnian system destruction, Han Solo death'
            WHEN planet_name = 'Ahch-To' THEN 'First Jedi temple, Luke exile'
            WHEN planet_name = 'Crait' THEN 'Battle of Crait, Luke projection'
            WHEN planet_name = 'Exegol' THEN 'Sith throne, Final Order fleet, Palpatine defeat'
            ELSE NULL
        END AS major_events,
        
        -- Expanded political affiliation
        CASE
            -- Republic/Empire affiliated
            WHEN planet_name IN ('Coruscant', 'Hosnian Prime', 'Chandrila', 'Corellia', 'Kamino') THEN 'Republic/Empire'
            -- Separatist affiliated
            WHEN planet_name IN ('Geonosis', 'Cato Neimoidia', 'Serenno', 'Skako', 'Raxus') THEN 'Separatist Alliance'
            -- Rebel/Resistance affiliated
            WHEN planet_name IN ('Yavin IV', 'Hoth', 'Dantooine', 'D''Qar', 'Ajan Kloss', 'Crait') THEN 'Rebel Alliance/Resistance'
            -- Neutral/Independent
            WHEN planet_name IN ('Tatooine', 'Mandalore', 'Naboo', 'Dagobah', 'Ahch-To', 'Bespin') THEN 'Neutral/Independent'
            -- Hutt/Criminal affiliated
            WHEN planet_name IN ('Nal Hutta', 'Nar Shaddaa', 'Kessel', 'Cantonica') THEN 'Hutt Space/Criminal Networks'
            -- First Order/Sith affiliated
            WHEN planet_name IN ('Exegol', 'Starkiller Base', 'Mustafar', 'Moraband', 'Malachor') THEN 'Sith/First Order'
            ELSE 'Unspecified Affiliation'
        END AS political_affiliation,
        
        -- Galactic significance rating (1-10 scale)
        CASE
            WHEN planet_name IN ('Coruscant', 'Exegol', 'Mustafar', 'Naboo', 'Tatooine', 
                             'Alderaan', 'Yavin IV', 'Hoth', 'Endor') THEN 10
            WHEN planet_name IN ('Geonosis', 'Kamino', 'Bespin', 'Dagobah', 'Utapau', 
                             'Jakku', 'Ahch-To', 'Starkiller Base', 'Crait') THEN 8
            WHEN planet_name IN ('Kashyyyk', 'Mon Cala', 'Mandalore', 'Ryloth', 'Dathomir',
                             'Corellia', 'Felucia', 'Scarif', 'Jedha') THEN 6
            WHEN planet_name IN ('Dantooine', 'Onderon', 'Bothawui', 'Lothal', 'Cantonica',
                             'Batuu', 'Mimban', 'Nevarro', 'Malachor') THEN 4
            ELSE 2
        END AS galactic_significance
    FROM planet_attributes
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(ps.planet_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS planet_key,
    
    -- Core identifiers
    ps.planet_id,
    ps.planet_name,
    
    -- Physical attributes with better error handling
    ps.diameter AS diameter_km,
    ps.rotation_period AS rotation_period_hours,
    ps.orbital_period AS orbital_period_days,
    
    -- Environmental attributes
    ps.climate,
    ps.climate_class,
    ps.gravity,
    ps.terrain,
    ps.terrain_class,
    ps.surface_water AS surface_water_percentage,
    
    -- Size and habitability
    ps.size_class,
    ps.habitability_score,
    
    -- Population data with better formatting and error handling
    CASE
        WHEN ps.population >= 1000000000 THEN TRIM(TO_CHAR(ps.population/1000000000.0, '999,999,990.9')) || ' billion'
        WHEN ps.population >= 1000000 THEN TRIM(TO_CHAR(ps.population/1000000.0, '999,999,990.9')) || ' million'
        WHEN ps.population >= 1000 THEN TRIM(TO_CHAR(ps.population/1000.0, '999,999,990.9')) || ' thousand'
        WHEN ps.population IS NOT NULL THEN TRIM(TO_CHAR(ps.population, '999,999,999,999'))
        ELSE 'unknown'
    END AS population_formatted,
    ps.population AS population_count,
    
    -- Add resident metrics from staging
    COALESCE(ps.resident_count, 0) AS known_resident_count,
    ps.resident_names AS notable_residents,
    
    -- Population density if both values are available
    CASE 
        WHEN ps.population IS NOT NULL AND ps.diameter IS NOT NULL AND ps.diameter > 0 THEN
            ROUND(ps.population / (3.14159 * POWER(ps.diameter/2, 2)), 2)
        ELSE NULL
    END AS population_density_per_km2,
    
    -- Habitability classification
    CASE
        WHEN ps.habitability_score >= 80 THEN 'Ideal'
        WHEN ps.habitability_score >= 60 THEN 'Highly Habitable'
        WHEN ps.habitability_score >= 40 THEN 'Moderately Habitable'
        WHEN ps.habitability_score >= 20 THEN 'Marginally Habitable'
        ELSE 'Barely Habitable/Hostile'
    END AS habitability_class,
    
    -- Terrain classifications from staging
    ps.is_temperate,
    ps.has_vegetation,
    ps.is_water_world,
    ps.is_desert_world,
    
    -- Galactic location and significance
    ps.galactic_region,
    ps.political_affiliation,
    ps.major_events,
    ps.galactic_significance,
    
    -- Enhanced key location flag with tiered importance
    CASE
        WHEN ps.planet_name IN ('Tatooine', 'Coruscant', 'Naboo', 'Mustafar', 'Alderaan', 
                             'Hoth', 'Endor', 'Dagobah', 'Yavin IV', 'Exegol') 
            THEN 'Primary Saga Location'
        WHEN ps.planet_name IN ('Geonosis', 'Kamino', 'Utapau', 'Kashyyyk', 'Bespin', 'Jakku',
                             'Ahch-To', 'Crait', 'Starkiller Base', 'Scarif', 'Jedha') 
            THEN 'Major Location'
        WHEN ps.planet_name IN ('Dantooine', 'Mon Cala', 'Ryloth', 'Mandalore', 'Corellia', 'Cantonica')
            THEN 'Notable Location'
        WHEN ps.galactic_significance >= 6
            THEN 'Significant Location'
        ELSE 'Standard Location'
    END AS location_importance,
    
    -- Films appearance data from staging model
    ps.film_names AS film_names_array,
    COALESCE(ps.film_appearances, 0) AS film_appearance_count,
    
    -- Source data metadata
    ps.url AS source_url,
    ps.fetch_timestamp,
    ps.processed_timestamp,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM planet_significance ps
ORDER BY ps.galactic_significance DESC, ps.habitability_score DESC