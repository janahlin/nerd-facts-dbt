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
  - Extracts pilot references from the nested arrays in starship data
  - Calculates pilot skill classifications and experience metrics
  - Identifies iconic starship-pilot combinations from the series
  - Provides context for starship functionality and operational roles
*/

WITH starship_pilots AS (
    -- Extract pilot references from the starships data with better error handling
    SELECT
        s.id AS starship_id,
        s.name AS starship_name,
        s.model,
        s.manufacturer,
        s.starship_class,
        s.cost_in_credits::NUMERIC AS cost,
        pilot_ref->>'url' AS pilot_url,
        -- Extract pilot ID from URL with error handling
        NULLIF(SPLIT_PART(COALESCE(pilot_ref->>'url', ''), '/', 6), '')::INTEGER AS pilot_id
    FROM {{ ref('stg_swapi_starships') }} s,  -- Corrected reference
    LATERAL jsonb_array_elements(
        CASE WHEN s.pilots IS NULL OR s.pilots = 'null' OR jsonb_array_length(s.pilots) = 0 
        THEN '[]'::jsonb ELSE s.pilots END
    ) AS pilot_ref
    WHERE s.id IS NOT NULL
),

-- Join with character information for context
pilot_details AS (
    SELECT
        sp.starship_id,
        sp.starship_name,
        sp.model,
        sp.manufacturer,
        sp.starship_class,
        sp.cost,
        sp.pilot_id,
        p.name AS pilot_name,
        p.species_id,
        p.gender,
        p.birth_year,
        -- Determine if force sensitive based on known Jedi/Sith
        CASE WHEN p.name IN (
            'Luke Skywalker', 'Darth Vader', 'Obi-Wan Kenobi', 'Anakin Skywalker', 
            'Yoda', 'Palpatine', 'Rey', 'Kylo Ren', 'Mace Windu', 'Qui-Gon Jinn',
            'Kit Fisto', 'Plo Koon', 'Luminara Unduli', 'Count Dooku', 'Darth Maul'
        ) THEN TRUE ELSE FALSE END AS force_sensitive,
        -- Calculate pilot's approximate age (if birth_year is available)
        CASE 
            WHEN p.birth_year ~ '^[0-9]+(\.[0-9]+)?$' THEN 
                CASE
                    WHEN p.name = 'Yoda' THEN 900  -- Special case for Yoda
                    ELSE COALESCE(p.birth_year::NUMERIC, 0)
                END
            ELSE 0
        END AS pilot_age
    FROM starship_pilots sp
    LEFT JOIN {{ ref('stg_swapi_people') }} p ON sp.pilot_id = p.id
),

-- Get species information to enhance pilot context
pilot_species AS (
    SELECT
        pd.*,
        s.name AS species_name,
        s.classification AS species_classification,
        s.average_lifespan
    FROM pilot_details pd
    LEFT JOIN {{ ref('stg_swapi_species') }} s ON pd.species_id = s.id
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
    
    -- Calculate the number of known starships piloted by this character
    COUNT(*) OVER (PARTITION BY ps.pilot_id) AS ships_piloted_count,
    
    -- Calculate pilot versatility based on how many different classes they pilot
    COUNT(DISTINCT ps.starship_class) OVER (PARTITION BY ps.pilot_id) AS ship_class_versatility,
    
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
    
    -- Era classification
    CASE
        WHEN ps.pilot_name IN ('Anakin Skywalker', 'Obi-Wan Kenobi', 'Padmé Amidala', 
                             'Qui-Gon Jinn', 'Mace Windu', 'Count Dooku', 'General Grievous') THEN 'Prequel Era'
        WHEN ps.pilot_name IN ('Luke Skywalker', 'Han Solo', 'Leia Organa', 'Darth Vader', 
                             'Chewbacca', 'Lando Calrissian') THEN 'Original Trilogy Era'
        WHEN ps.pilot_name IN ('Rey', 'Finn', 'Poe Dameron', 'Kylo Ren') THEN 'Sequel Era'
        WHEN ps.pilot_name IN ('Din Djarin') THEN 'Mandalorian Era'
        ELSE 'Unknown Era'
    END AS story_era,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM pilot_species ps
WHERE ps.pilot_id IS NOT NULL AND ps.starship_id IS NOT NULL
ORDER BY ps.starship_name, pilot_skill DESC, ps.pilot_name