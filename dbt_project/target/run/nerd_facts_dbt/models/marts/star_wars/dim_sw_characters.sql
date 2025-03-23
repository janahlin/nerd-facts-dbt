
  
    

  create  table "nerd_facts"."public"."dim_sw_characters__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: dim_sw_characters
  Description: Character dimension table with enriched attributes
*/

WITH character_base AS (
  SELECT
    p.people_id,
    p.name,
    p.height,
    p.mass,
    p.hair_color,
    p.skin_color,
    p.eye_color,
    p.birth_year,
    p.gender,
    p.homeworld_id   -- Fixed: Changed from p.homeworld to p.homeworld_id
  FROM "nerd_facts"."public"."int_swapi_people" p
),

-- Get homeworld information
homeworld_info AS (
  SELECT 
    p.people_id,
    pl.name AS homeworld_name
  FROM "nerd_facts"."public"."int_swapi_people" p
  LEFT JOIN "nerd_facts"."public"."int_swapi_planets" pl ON p.homeworld_id::INTEGER = pl.planet_id
),

-- Calculate film appearances
film_appearances AS (
  SELECT
    fc.people_id,
    COUNT(DISTINCT fc.film_id) AS film_count,
    STRING_AGG(f.title, ', ' ORDER BY f.episode_id) AS film_appearances
  FROM "nerd_facts"."public"."int_swapi_films_characters" fc
  JOIN "nerd_facts"."public"."int_swapi_films" f ON fc.film_id = f.film_id
  GROUP BY fc.people_id
)

SELECT
  -- Primary Key
  md5(cast(coalesce(cast(cb.people_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
  
  -- Natural Key
  cb.people_id,
  
  -- Character Attributes
  cb.name AS character_name,
  cb.height::NUMERIC AS height_cm,
  cb.mass::NUMERIC AS mass_kg,
  cb.hair_color,
  cb.skin_color,
  cb.eye_color,
  cb.birth_year,
  cb.gender,
  
  -- Homeworld information
  hi.homeworld_name,
  
  -- Film appearances
  COALESCE(fa.film_count, 0) AS film_count,
  COALESCE(fa.film_appearances, 'None') AS film_appearances,
  
  -- Character Classification
  CASE
    -- Protagonist/Antagonist tier - central characters
    WHEN cb.name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 
                     'Rey', 'Kylo Ren', 'Din Djarin', 'Grogu') 
        THEN 'Protagonist/Antagonist'
    
    -- Major characters
    WHEN cb.name IN ('Han Solo', 'Leia Organa', 'Obi-Wan Kenobi', 
                    'Emperor Palpatine', 'Finn', 'Poe Dameron',
                    'Padmé Amidala', 'Count Dooku', 'Qui-Gon Jinn',
                    'Darth Maul', 'Jyn Erso', 'Cassian Andor', 'Boba Fett') 
        THEN 'Major'
    
    -- Supporting characters
    WHEN cb.name IN ('Chewbacca', 'C-3PO', 'R2-D2', 'Yoda', 
                    'Lando Calrissian', 'Mace Windu', 'General Grievous', 
                    'Admiral Ackbar', 'BB-8', 'General Hux', 'Rose Tico',
                    'Moff Gideon', 'Saw Gerrera', 'Jabba the Hutt',
                    'Ahsoka Tano') 
        THEN 'Supporting'
                                
    -- Characters with many appearances are likely more important
    WHEN COALESCE(fa.film_count, 0) >= 3
        THEN 'Notable'
    
    -- Characters with more details are likely more important
    WHEN cb.gender IS NOT NULL AND cb.birth_year IS NOT NULL 
        THEN 'Notable'
    
    ELSE 'Minor'
  END AS character_importance_tier,
  
  -- Character alignment
  CASE 
    WHEN cb.name IN ('Darth Vader', 'Emperor Palpatine', 'Darth Maul',
                        'Count Dooku', 'General Grievous', 'Kylo Ren',
                        'Captain Phasma', 'General Hux', 'Moff Tarkin',
                        'Jabba the Hutt', 'Boba Fett', 'Jango Fett')
        THEN 'Villain'
        
    WHEN cb.name IN ('Luke Skywalker', 'Leia Organa', 'Han Solo',
                        'Obi-Wan Kenobi', 'Yoda', 'Rey', 'Finn', 'Poe Dameron',
                        'Padmé Amidala', 'Qui-Gon Jinn', 'Mace Windu')
        THEN 'Hero'
        
    WHEN cb.name IN ('Lando Calrissian', 'Anakin Skywalker')
        THEN 'Ambiguous'
        
    ELSE 'Neutral'
  END AS character_alignment,
  
  -- Character Type
  CASE 
    WHEN cb.name LIKE '%Droid%' OR cb.name IN ('C-3PO', 'R2-D2', 'BB-8') THEN 'Droid'
    WHEN cb.name IN ('Jabba the Hutt') THEN 'Hutt'
    WHEN cb.name IN ('Yoda', 'Grogu') THEN 'Unknown Species'
    WHEN cb.name IN ('Chewbacca') THEN 'Wookiee'
    ELSE 'Humanoid'
  END AS character_type,
  
  -- Force user
  CASE 
    WHEN cb.name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 'Rey', 'Kylo Ren',
                   'Obi-Wan Kenobi', 'Emperor Palpatine', 'Yoda', 'Mace Windu',
                   'Count Dooku', 'Qui-Gon Jinn', 'Darth Maul', 'Ahsoka Tano')
    THEN TRUE
    ELSE FALSE
  END AS is_force_user,
  
  -- Force alignment
  CASE 
    WHEN cb.name IN ('Darth Vader', 'Emperor Palpatine', 'Darth Maul', 
                   'Count Dooku', 'Kylo Ren')
    THEN 'Dark Side'
    
    WHEN cb.name IN ('Luke Skywalker', 'Obi-Wan Kenobi', 'Yoda', 'Rey',
                   'Mace Windu', 'Qui-Gon Jinn', 'Ahsoka Tano') 
    THEN 'Light Side'
    
    WHEN cb.name IN ('Anakin Skywalker')
    THEN 'Both (Changed)'
    
    ELSE NULL
  END AS force_alignment,
  
  -- Time Dimension
  CURRENT_TIMESTAMP AS dbt_loaded_at

FROM character_base cb
LEFT JOIN homeworld_info hi ON cb.people_id = hi.people_id
LEFT JOIN film_appearances fa ON cb.people_id = fa.people_id
WHERE cb.people_id IS NOT NULL
ORDER BY 
  CASE 
    WHEN COALESCE(fa.film_count, 0) >= 3 THEN 0
    ELSE 1
  END,
  COALESCE(fa.film_count, 0) DESC,
  cb.name
  );
  