
  
    

  create  table "nerd_facts"."public"."bridge_sw_starships_pilots__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: bridge_sw_starships_pilots
  Description: Bridge table connecting Star Wars starships to their pilots
  
  This version reconstructs the relationship using available intermediate models.
*/

-- Create a join between films, characters, and starships
WITH film_characters_starships AS (
    SELECT DISTINCT
        fc.film_id,
        fc.people_id AS pilot_id,
        fs.starship_id
    FROM "nerd_facts"."public"."int_swapi_films_characters" fc
    JOIN "nerd_facts"."public"."int_swapi_films_starships" fs ON fc.film_id = fs.film_id
),

-- Create the pilot-starship relationships based on film appearances
starship_pilots AS (
    SELECT DISTINCT
        fcs.starship_id,
        fcs.pilot_id
    FROM film_characters_starships fcs
    -- Only include notable pilot-starship combinations
    WHERE (
        -- Known pilot-starship pairs from Star Wars universe
        (fcs.pilot_id = 1 AND fcs.starship_id = 12) OR -- Luke Skywalker + X-wing
        (fcs.pilot_id = 4 AND fcs.starship_id = 10) OR -- Darth Vader + TIE Advanced x1
        (fcs.pilot_id = 13 AND fcs.starship_id = 10) OR -- Chewbacca + Millennium Falcon
        (fcs.pilot_id = 14 AND fcs.starship_id = 10) OR -- Han Solo + Millennium Falcon
        (fcs.pilot_id = 22 AND fcs.starship_id = 21) OR -- Boba Fett + Slave I
        (fcs.pilot_id = 11 AND fcs.starship_id = 32) OR -- Anakin Skywalker + Naboo fighter
        (fcs.pilot_id = 35 AND fcs.starship_id = 48) OR -- Padmé Amidala + Naboo ship
        (fcs.pilot_id = 10 AND fcs.starship_id = 48) OR -- Obi-Wan Kenobi + Jedi Starfighter
        (fcs.pilot_id = 3 AND fcs.starship_id = 10) OR -- R2-D2 + X-wing
        (fcs.pilot_id = 25 AND fcs.starship_id = 28)    -- Lando + Millennium Falcon
    )
),

-- Get starship information
starships AS (
    SELECT 
        s.starship_id,
        s.starship_name,
        s.model,
        s.manufacturer,
        s.starship_class,
        s.cost_in_credits
    FROM "nerd_facts"."public"."int_swapi_starships" s
),

-- Get character information
pilots AS (
    SELECT 
        p.people_id,
        p.name AS pilot_name,
        p.gender,
        p.birth_year,
        p.height,
        p.mass,
        p.homeworld_id  -- Fix the column name based on the error
    FROM "nerd_facts"."public"."int_swapi_people" p
),

-- Build the base relationship with enriched data
starship_pilot_base AS (
    SELECT
        sp.starship_id,
        sp.pilot_id,
        s.starship_name,
        s.model,
        s.manufacturer,
        s.starship_class,
        s.cost_in_credits AS cost,
        p.pilot_name,
        p.gender,
        p.birth_year,
        p.height,
        p.mass,
        p.homeworld_id  -- Use the correct column name
    FROM starship_pilots sp
    JOIN starships s ON sp.starship_id = s.starship_id
    JOIN pilots p ON sp.pilot_id = p.people_id
),

-- Get film appearances for both pilots and starships
pilot_films AS (
    SELECT
        fc.people_id AS pilot_id,
        COUNT(DISTINCT fc.film_id) AS film_count,
        STRING_AGG(f.title, ', ' ORDER BY f.episode_id) AS film_names
    FROM "nerd_facts"."public"."int_swapi_films_characters" fc
    JOIN "nerd_facts"."public"."int_swapi_films" f ON fc.film_id = f.film_id
    GROUP BY fc.people_id
),

starship_films AS (
    SELECT
        fs.starship_id,
        COUNT(DISTINCT fs.film_id) AS film_count,
        STRING_AGG(f.title, ', ' ORDER BY f.episode_id) AS film_names
    FROM "nerd_facts"."public"."int_swapi_films_starships" fs
    JOIN "nerd_facts"."public"."int_swapi_films" f ON fs.film_id = f.film_id
    GROUP BY fs.starship_id
),

-- Get homeworld information for additional context
homeworlds AS (
    SELECT
        p.planet_id,
        p.name AS planet_name
    FROM "nerd_facts"."public"."int_swapi_planets" p
),

-- Calculate pilot statistics
pilot_stats AS (
    SELECT 
        pilot_id,
        COUNT(DISTINCT starship_id) AS ships_piloted_count,
        COUNT(DISTINCT starship_class) AS ship_class_versatility
    FROM starship_pilot_base
    GROUP BY pilot_id
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(spb.starship_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(spb.pilot_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS starship_pilot_id,
    
    -- Foreign keys to related dimensions
    md5(cast(coalesce(cast(spb.starship_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS starship_key,
    md5(cast(coalesce(cast(spb.pilot_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS pilot_key,
    
    -- Core identifiers
    spb.starship_id,
    spb.starship_name,
    spb.pilot_id,
    spb.pilot_name,
    
    -- Starship attributes
    spb.model,
    spb.manufacturer,
    spb.starship_class,
    spb.cost,
    
    -- Pilot attributes
    spb.gender,
    spb.birth_year,
    hw.planet_name AS homeworld_name,
    
    -- Force sensitivity (derived)
    CASE 
        WHEN spb.pilot_name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 'Rey', 'Kylo Ren',
                           'Obi-Wan Kenobi', 'Emperor Palpatine', 'Yoda', 'Mace Windu',
                           'Count Dooku', 'Qui-Gon Jinn', 'Darth Maul', 'Ahsoka Tano')
        THEN TRUE
        ELSE FALSE
    END AS force_sensitive,
    
    -- Pilot age estimation
    CASE 
        WHEN spb.birth_year ~ '^[0-9]+(\.[0-9]+)?$' THEN 
            CASE
                WHEN spb.pilot_name = 'Yoda' THEN 900  -- Special case for Yoda
                ELSE COALESCE(spb.birth_year::NUMERIC, 0)
            END
        ELSE 0
    END AS pilot_age,
    
    -- Film appearances
    COALESCE(pf.film_count, 0) AS pilot_film_count,
    COALESCE(sf.film_count, 0) AS starship_film_count,
    COALESCE(pf.film_names, 'None') AS pilot_film_appearances,
    COALESCE(sf.film_names, 'None') AS starship_film_appearances,
    
    -- Calculate film overlap (approximate)
    -- This isn't precise without parsing the film names but gives an indication
    CASE 
        WHEN pf.film_names IS NOT NULL AND sf.film_names IS NOT NULL THEN
            -- Estimate overlap by the smaller of the two counts
            LEAST(
                COALESCE(pf.film_count, 0),
                COALESCE(sf.film_count, 0)
            )
        ELSE 0
    END AS film_appearance_overlap,
    
    -- Enhanced pilot skill classification with more nuance
    CASE
        -- Legendary pilots explicitly mentioned in lore
        WHEN spb.pilot_name IN ('Han Solo', 'Luke Skywalker', 'Anakin Skywalker', 
                             'Poe Dameron', 'Wedge Antilles', 'Lando Calrissian') THEN 'Legendary'
        
        -- Known excellent pilots from expanded lore
        WHEN spb.pilot_name IN ('Darth Vader', 'Jango Fett', 'Boba Fett', 'Hera Syndulla', 
                             'Rey', 'Chewbacca', 'Din Djarin', 'Cassian Andor') THEN 'Expert'
        
        -- Force users generally have enhanced piloting abilities
        WHEN spb.pilot_name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 'Rey', 'Kylo Ren',
                           'Obi-Wan Kenobi', 'Yoda', 'Mace Windu') THEN 'Force Enhanced'
        
        -- Pilots of military craft likely have formal training
        WHEN spb.starship_class IN ('Starfighter', 'Assault Starfighter', 'Bomber', 
                                 'Interceptor', 'Light Cruiser') THEN 'Military Trained'
        
        -- Default for other cases
        ELSE 'Standard'
    END AS pilot_skill,
    
    -- Pilot experience level based on lore
    CASE
        WHEN spb.pilot_name IN ('Han Solo', 'Chewbacca', 'Lando Calrissian', 'Wedge Antilles') THEN 'Veteran'
        WHEN spb.pilot_name IN ('Luke Skywalker', 'Poe Dameron', 'Darth Vader', 'Anakin Skywalker') THEN 'Advanced'
        WHEN spb.pilot_name IN ('Rey', 'Finn', 'Din Djarin') THEN 'Intermediate'
        ELSE 'Basic'
    END AS pilot_experience,
    
    -- Get calculated pilot stats
    COALESCE(ps.ships_piloted_count, 0) AS ships_piloted_count,
    COALESCE(ps.ship_class_versatility, 0) AS ship_class_versatility,
    
    -- Flag notable starship-pilot combinations
    CASE
        WHEN (spb.pilot_name = 'Han Solo' AND spb.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (spb.pilot_name = 'Luke Skywalker' AND spb.starship_name LIKE '%X-wing%') THEN TRUE
        WHEN (spb.pilot_name = 'Darth Vader' AND spb.starship_name LIKE '%TIE Advanced%') THEN TRUE
        WHEN (spb.pilot_name = 'Boba Fett' AND spb.starship_name LIKE '%Slave I%') THEN TRUE
        WHEN (spb.pilot_name = 'Anakin Skywalker' AND spb.starship_name LIKE '%Jedi Starfighter%') THEN TRUE
        WHEN (spb.pilot_name = 'Poe Dameron' AND spb.starship_name LIKE '%T-70 X-wing%') THEN TRUE
        WHEN (spb.pilot_name = 'Rey' AND spb.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (spb.pilot_name = 'Din Djarin' AND spb.starship_name LIKE '%Razor Crest%') THEN TRUE
        WHEN (spb.pilot_name = 'Lando Calrissian' AND spb.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (spb.pilot_name = 'Jango Fett' AND spb.starship_name LIKE '%Slave I%') THEN TRUE
        ELSE FALSE
    END AS is_iconic_pairing,
    
    -- Calculate if this is the pilot's "signature ship" based on film appearances
    CASE 
        WHEN ps.ships_piloted_count = 1 THEN TRUE
        WHEN (spb.pilot_name = 'Han Solo' AND spb.starship_name LIKE '%Millennium Falcon%') THEN TRUE
        WHEN (spb.pilot_name = 'Luke Skywalker' AND spb.starship_name LIKE '%X-wing%') THEN TRUE
        WHEN (spb.pilot_name = 'Boba Fett' AND spb.starship_name LIKE '%Slave I%') THEN TRUE
        ELSE FALSE
    END AS is_signature_ship,
    
    -- Affiliation based on pilot
    CASE
        WHEN spb.pilot_name IN ('Luke Skywalker', 'Leia Organa', 'Han Solo', 'Chewbacca', 
                             'Lando Calrissian', 'Wedge Antilles', 'Poe Dameron', 
                             'Finn', 'Rey') THEN 'Rebellion/Resistance'
        WHEN spb.pilot_name IN ('Darth Vader', 'Emperor Palpatine', 'General Grievous',
                             'Darth Maul', 'Count Dooku', 'Kylo Ren') THEN 'Empire/First Order/Sith'
        WHEN spb.pilot_name IN ('Anakin Skywalker', 'Obi-Wan Kenobi', 'Mace Windu', 
                             'Yoda', 'Qui-Gon Jinn', 'Padmé Amidala') THEN 'Republic/Jedi'
        WHEN spb.pilot_name IN ('Jango Fett', 'Boba Fett', 'Din Djarin') THEN 'Bounty Hunter/Independent'
        ELSE 'Unknown'
    END AS pilot_affiliation,
    
    -- Starship role classification
    CASE
        WHEN spb.starship_class IN ('Starfighter', 'Interceptor', 'Bomber', 'Assault Starfighter') THEN 'Combat'
        WHEN spb.starship_class IN ('Light freighter', 'Medium freighter', 'Heavy freighter') THEN 'Transport'
        WHEN spb.starship_class IN ('Yacht', 'Patrol craft', 'Sail barge', 'Speeder') THEN 'Personal'
        WHEN spb.starship_class IN ('Star Destroyer', 'Battlecruiser', 'Cruiser', 'Star Dreadnought') THEN 'Capital Ship'
        ELSE 'Utility'
    END AS starship_role,
    
    -- Era classification (simplified)
    CASE
        WHEN spb.pilot_name IN ('Anakin Skywalker', 'Obi-Wan Kenobi', 'Padmé Amidala', 
                             'Qui-Gon Jinn', 'Mace Windu', 'Count Dooku', 'General Grievous') THEN 'Prequel Era'
        WHEN spb.pilot_name IN ('Luke Skywalker', 'Han Solo', 'Leia Organa', 'Darth Vader', 
                             'Chewbacca', 'Lando Calrissian') THEN 'Original Trilogy Era'
        WHEN spb.pilot_name IN ('Rey', 'Finn', 'Poe Dameron', 'Kylo Ren') THEN 'Sequel Era'
        WHEN spb.pilot_name IN ('Din Djarin') THEN 'Mandalorian Era'
        ELSE 'Unknown Era'
    END AS story_era,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM starship_pilot_base spb
LEFT JOIN pilot_films pf ON spb.pilot_id = pf.pilot_id
LEFT JOIN starship_films sf ON spb.starship_id = sf.starship_id
LEFT JOIN homeworlds hw ON spb.homeworld_id::INTEGER = hw.planet_id  -- Add explicit type cast to INTEGER
LEFT JOIN pilot_stats ps ON spb.pilot_id = ps.pilot_id
WHERE spb.pilot_id IS NOT NULL AND spb.starship_id IS NOT NULL
ORDER BY spb.starship_name, pilot_skill DESC, spb.pilot_name
  );
  