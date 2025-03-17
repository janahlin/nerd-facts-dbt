{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['starship_id']}, {'columns': ['pilot_id']}, {'columns': ['starship_pilot_id']}],
    unique_key = 'starship_pilot_id'
  )
}}

/*
  Model: bridge_sw_starships_pilots
  Description: Bridge table connecting Star Wars starships to their pilots
  
  Notes:
  - Handles the many-to-many relationship between starships and pilots
  - Simplified to work with the current state of our staging models
  - NULL fields and references have been handled appropriately
*/

WITH direct_connections AS (
    -- Extract pilot references from the starships data
    SELECT
        s.id AS starship_id,
        s.starship_name,
        s.model,
        s.manufacturer,
        s.starship_class,
        s.cost_in_credits AS cost,
        -- Extract pilot ID from URL with better error handling
        NULLIF(SPLIT_PART(pilot_ref->>'url', '/', 6), '')::INTEGER AS pilot_id
    FROM {{ ref('stg_swapi_starships') }} s,
    LATERAL jsonb_array_elements(
        CASE WHEN s.pilots IS NULL OR s.pilots = 'null' THEN '[]'::jsonb
        ELSE s.pilots END
    ) AS pilot_ref
    WHERE s.id IS NOT NULL
),

-- Join with character information for context
pilot_details AS (
    SELECT
        dc.starship_id,
        dc.starship_name,
        dc.model,
        dc.manufacturer,
        dc.starship_class,
        dc.cost,
        dc.pilot_id,
        p.name AS pilot_name,
        -- Using NULL here since species_id isn't available yet
        NULL::INTEGER AS species_id,
        p.gender,
        p.birth_year,
        p.film_appearances, -- Add film appearance count
        NULL AS film_names,  -- Placeholder for now
        -- Determine if force sensitive based on known Jedi/Sith
        p.force_sensitive,
        -- Calculate pilot's approximate age (if birth_year is available)
        CASE 
            WHEN p.birth_year ~ '^[0-9]+(\.[0-9]+)?$' THEN 
                CASE
                    WHEN p.name = 'Yoda' THEN 900  -- Special case for Yoda
                    ELSE COALESCE(p.birth_year::NUMERIC, 0)
                END
            ELSE 0
        END AS pilot_age,
        -- Placeholder for character_era
        NULL AS character_era
    FROM direct_connections dc
    LEFT JOIN {{ ref('stg_swapi_people') }} p ON dc.pilot_id = p.id
),

-- Get species information to enhance pilot context (skipping the join for now)
pilot_species AS (
    SELECT
        pd.*,
        NULL AS species_name,
        NULL AS species_classification,
        NULL AS average_lifespan,
        -- Get starship's film appearances
        (SELECT sh.film_appearances FROM {{ ref('stg_swapi_starships') }} sh 
         WHERE sh.id = pd.starship_id) AS starship_film_count,
        -- Placeholder for film_names
        NULL AS starship_film_names
    FROM pilot_details pd
    -- Skip this join for now since species_id isn't properly set up
    -- LEFT JOIN {{ ref('stg_swapi_species') }} s ON pd.species_id = s.id
),

-- Calculate pilot statistics separately to avoid using DISTINCT in window functions
pilot_stats AS (
    SELECT 
        pilot_id,
        COUNT(*) AS ships_piloted_count,
        -- Use array_agg and then array_length to count distinct values
        -- This is a workaround for not being able to use DISTINCT in window functions
        ARRAY_LENGTH(ARRAY_AGG(DISTINCT starship_class), 1) AS ship_class_versatility
    FROM pilot_details
    WHERE pilot_id IS NOT NULL
    GROUP BY pilot_id
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['ps.starship_id', 'ps.pilot_id']) }} AS starship_pilot_id,
    
    -- Foreign keys to related dimensions
    {{ dbt_utils.generate_surrogate_key(['ps.starship_id']) }} AS starship_key,
    {{ dbt_utils.generate_surrogate_key(['ps.pilot_id']) }} AS pilot_key,
    
    -- Core identifiers
    ps.starship_id,
    ps.starship_name,
    ps.pilot_id,
    ps.pilot_name,
    
    -- Starship attributes
    ps.model,
    ps.manufacturer,
    ps.starship_class,
    ps.cost,
    
    -- Pilot attributes
    ps.species_name,
    ps.gender,
    ps.birth_year,
    ps.force_sensitive,
    ps.pilot_age,
    
    -- Film appearances
    ps.film_appearances AS pilot_film_count,
    ps.starship_film_count,
    
    -- Placeholder for film overlap, since we don't have film_names
    0 AS film_appearance_overlap,
    
    -- Enhanced pilot skill classification with more nuance
    CASE
        -- Legendary pilots explicitly mentioned in lore
        WHEN ps.pilot_name IN ('Han Solo', 'Luke Skywalker', 'Anakin Skywalker', 
                             'Poe Dameron', 'Wedge Antilles', 'Lando Calrissian') THEN 'Legendary'
        
        -- Known excellent pilots from expanded lore
        WHEN ps.pilot_name IN ('Darth Vader', 'Jango Fett', 'Boba Fett', 'Hera Syndulla', 
                             'Rey', 'Chewbacca', 'Din Djarin', 'Cassian Andor') THEN 'Expert'
        
        -- Force users generally have enhanced piloting abilities
        WHEN ps.force_sensitive THEN 'Force Enhanced'
        
        -- Pilots of military craft likely have formal training
        WHEN ps.starship_class IN ('Starfighter', 'Assault Starfighter', 'Bomber', 
                                 'Interceptor', 'Light Cruiser') THEN 'Military Trained'
        
        -- Default for other cases
        ELSE 'Standard'
    END AS pilot_skill,
    
    -- Pilot experience level based on lore
    CASE
        WHEN ps.pilot_name IN ('Han Solo', 'Chewbacca', 'Lando Calrissian', 'Wedge Antilles') THEN 'Veteran'
        WHEN ps.pilot_name IN ('Luke Skywalker', 'Poe Dameron', 'Darth Vader', 'Anakin Skywalker') THEN 'Advanced'
        WHEN ps.pilot_name IN ('Rey', 'Finn', 'Din Djarin') THEN 'Intermediate'
        ELSE 'Basic'
    END AS pilot_experience,
    
    -- Get calculated pilot stats
    COALESCE(pstat.ships_piloted_count, 0) AS ships_piloted_count,
    COALESCE(pstat.ship_class_versatility, 0) AS ship_class_versatility,
    
    -- Flag notable starship-pilot combinations with expanded list
    CASE
        WHEN (ps.pilot_name = 'Han Solo' AND ps.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (ps.pilot_name = 'Luke Skywalker' AND ps.starship_name LIKE '%X-wing%') THEN TRUE
        WHEN (ps.pilot_name = 'Darth Vader' AND ps.starship_name LIKE '%TIE Advanced%') THEN TRUE
        WHEN (ps.pilot_name = 'Boba Fett' AND ps.starship_name LIKE '%Slave I%') THEN TRUE
        WHEN (ps.pilot_name = 'Anakin Skywalker' AND ps.starship_name LIKE '%Jedi Starfighter%') THEN TRUE
        WHEN (ps.pilot_name = 'Poe Dameron' AND ps.starship_name LIKE '%T-70 X-wing%') THEN TRUE
        WHEN (ps.pilot_name = 'Rey' AND ps.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (ps.pilot_name = 'Din Djarin' AND ps.starship_name LIKE '%Razor Crest%') THEN TRUE
        WHEN (ps.pilot_name = 'Lando Calrissian' AND ps.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (ps.pilot_name = 'Jango Fett' AND ps.starship_name LIKE '%Slave I%') THEN TRUE
        ELSE FALSE
    END AS is_iconic_pairing,
    
    -- Calculate if this is the pilot's "signature ship" (simplified)
    FALSE AS is_signature_ship,
    
    -- Affiliation based on pilot
    CASE
        WHEN ps.pilot_name IN ('Luke Skywalker', 'Leia Organa', 'Han Solo', 'Chewbacca', 
                             'Lando Calrissian', 'Wedge Antilles', 'Poe Dameron', 
                             'Finn', 'Rey') THEN 'Rebellion/Resistance'
        WHEN ps.pilot_name IN ('Darth Vader', 'Emperor Palpatine', 'General Grievous',
                             'Darth Maul', 'Count Dooku', 'Kylo Ren') THEN 'Empire/First Order/Sith'
        WHEN ps.pilot_name IN ('Anakin Skywalker', 'Obi-Wan Kenobi', 'Mace Windu', 
                             'Yoda', 'Qui-Gon Jinn', 'Padmé Amidala') THEN 'Republic/Jedi'
        WHEN ps.pilot_name IN ('Jango Fett', 'Boba Fett', 'Din Djarin') THEN 'Bounty Hunter/Independent'
        ELSE 'Unknown'
    END AS pilot_affiliation,
    
    -- Starship role classification
    CASE
        WHEN ps.starship_class IN ('Starfighter', 'Interceptor', 'Bomber', 'Assault Starfighter') THEN 'Combat'
        WHEN ps.starship_class IN ('Light freighter', 'Medium freighter', 'Heavy freighter') THEN 'Transport'
        WHEN ps.starship_class IN ('Yacht', 'Patrol craft', 'Sail barge', 'Speeder') THEN 'Personal'
        WHEN ps.starship_class IN ('Star Destroyer', 'Battlecruiser', 'Cruiser', 'Star Dreadnought') THEN 'Capital Ship'
        ELSE 'Utility'
    END AS starship_role,
    
    -- Era classification (simplified)
    CASE
        WHEN ps.pilot_name IN ('Anakin Skywalker', 'Obi-Wan Kenobi', 'Padmé Amidala', 
                             'Qui-Gon Jinn', 'Mace Windu', 'Count Dooku', 'General Grievous') THEN 'Prequel Era'
        WHEN ps.pilot_name IN ('Luke Skywalker', 'Han Solo', 'Leia Organa', 'Darth Vader', 
                             'Chewbacca', 'Lando Calrissian') THEN 'Original Trilogy Era'
        WHEN ps.pilot_name IN ('Rey', 'Finn', 'Poe Dameron', 'Kylo Ren') THEN 'Sequel Era'
        WHEN ps.pilot_name IN ('Din Djarin') THEN 'Mandalorian Era'
        ELSE 'Unknown Era'
    END AS story_era,
    
    -- Add source URL tracking
    (SELECT s.url FROM {{ ref('stg_swapi_starships') }} s WHERE s.id = ps.starship_id) AS starship_url,
    (SELECT p.url FROM {{ ref('stg_swapi_people') }} p WHERE p.id = ps.pilot_id) AS pilot_url,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM pilot_species ps
LEFT JOIN pilot_stats pstat ON ps.pilot_id = pstat.pilot_id
WHERE ps.pilot_id IS NOT NULL AND ps.starship_id IS NOT NULL
ORDER BY ps.starship_name, pilot_skill DESC, ps.pilot_name