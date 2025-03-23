
  
    

  create  table "nerd_facts"."public"."bridge_sw_characters_films__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: bridge_sw_characters_films
  Description: Bridge table connecting characters and films with enriched relationship attributes
*/

WITH character_film_base AS (
  SELECT
    fc.film_id,
    fc.people_id AS character_id,
    f.title AS film_title,
    f.episode_id,
    p.name AS character_name,
    f.release_date,
    p.gender,
    p.birth_year
  FROM "nerd_facts"."public"."int_swapi_films_characters" fc
  JOIN "nerd_facts"."public"."int_swapi_films" f ON fc.film_id = f.film_id
  JOIN "nerd_facts"."public"."int_swapi_people" p ON fc.people_id = p.people_id
),

-- Character importance tiers
character_importance AS (
  SELECT 
    cfb.*,
    CASE
      -- Protagonist/Antagonist tier - central characters
      WHEN cfb.character_name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 
                              'Rey', 'Kylo Ren', 'Din Djarin', 'Grogu') 
        THEN 'Protagonist/Antagonist'
      
      -- Major characters
      WHEN cfb.character_name IN ('Han Solo', 'Leia Organa', 'Obi-Wan Kenobi', 
                              'Emperor Palpatine', 'Finn', 'Poe Dameron',
                              'Padmé Amidala', 'Count Dooku', 'Qui-Gon Jinn',
                              'Darth Maul', 'Jyn Erso', 'Cassian Andor', 'Boba Fett') 
        THEN 'Major'
      
      -- Supporting characters
      WHEN cfb.character_name IN ('Chewbacca', 'C-3PO', 'R2-D2', 'Yoda', 
                              'Lando Calrissian', 'Mace Windu', 'General Grievous', 
                              'Admiral Ackbar', 'BB-8', 'General Hux', 'Rose Tico',
                              'Moff Gideon', 'Saw Gerrera', 'Jabba the Hutt',
                              'Ahsoka Tano') 
        THEN 'Supporting'
                              
      -- Additional major characters by ID
      WHEN cfb.character_id IN (1, 2, 3, 4, 5, 10, 11, 13) 
        THEN 'Major'
      
      -- Characters with more details are likely more important
      WHEN cfb.gender IS NOT NULL AND cfb.birth_year IS NOT NULL 
        THEN 'Notable'
      
      ELSE 'Minor'
    END AS character_importance_tier
  FROM character_film_base cfb
),

-- Add trilogy and film appearance information
character_film_enriched AS (
  SELECT 
    ci.*,
    -- Trilogy classification
    CASE
      WHEN ci.episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
      WHEN ci.episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy'
      WHEN ci.episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
      ELSE 'Anthology Films'
    END AS trilogy,
    
    -- Extract year for easier querying
    EXTRACT(YEAR FROM ci.release_date) AS release_year,
    
    -- Character appearance metrics
    COUNT(*) OVER (PARTITION BY ci.character_id) AS character_film_count,
    (SELECT COUNT(DISTINCT film_id) FROM "nerd_facts"."public"."int_swapi_films") AS total_films,
    
    -- Order of appearance
    ROW_NUMBER() OVER (
      PARTITION BY ci.character_id 
      ORDER BY COALESCE(ci.episode_id, 999)
    ) AS chronological_appearance_number,
    
    ROW_NUMBER() OVER (
      PARTITION BY ci.character_id 
      ORDER BY ci.release_date
    ) AS release_order_appearance_number
  FROM character_importance ci
)

SELECT
  -- Primary Key
  md5(cast(coalesce(cast(cfe.character_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(cfe.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_film_key,
  
  -- Foreign Keys
  md5(cast(coalesce(cast(cfe.character_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
  md5(cast(coalesce(cast(cfe.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS film_key,
  
  -- Source IDs
  cfe.character_id,
  cfe.film_id,
  
  -- Basic Attributes
  cfe.character_name,
  cfe.film_title,
  cfe.episode_id,
  
  -- Character role in film
  cfe.character_importance_tier AS character_role,
  
  -- Film categorization
  cfe.trilogy AS film_saga,
  cfe.release_year,
  
  -- Appearance metrics
  cfe.chronological_appearance_number,
  cfe.release_order_appearance_number,
  cfe.character_film_count,
  
  -- Character appearance percentage across all films
  ROUND((cfe.character_film_count * 100.0 / NULLIF(cfe.total_films, 0)), 1) AS saga_appearance_percentage,
  
  -- Timeline attributes
  CASE WHEN cfe.chronological_appearance_number = 1 THEN TRUE ELSE FALSE END AS is_first_chronological_appearance,
  CASE WHEN cfe.release_order_appearance_number = 1 THEN TRUE ELSE FALSE END AS is_first_release_appearance,
  
  -- Narrative role in specific films
  CASE 
    WHEN cfe.character_name IN ('Darth Vader', 'Anakin Skywalker') AND 
         cfe.episode_id BETWEEN 1 AND 3 THEN 'Protagonist'
    WHEN cfe.character_name IN ('Darth Vader') AND 
         cfe.episode_id BETWEEN 4 AND 6 THEN 'Antagonist'
    WHEN cfe.character_name = 'Luke Skywalker' AND 
         cfe.episode_id BETWEEN 4 AND 6 THEN 'Protagonist'
    WHEN cfe.character_name = 'Rey' AND 
         cfe.episode_id BETWEEN 7 AND 9 THEN 'Protagonist'
    WHEN cfe.character_name = 'Kylo Ren' AND 
         cfe.episode_id BETWEEN 7 AND 8 THEN 'Antagonist'
    WHEN cfe.character_name = 'Kylo Ren' AND 
         cfe.episode_id = 9 THEN 'Protagonist'
    WHEN cfe.character_importance_tier = 'Protagonist/Antagonist' THEN 'Key Character'
    WHEN cfe.character_importance_tier = 'Major' THEN 'Major Character'
    ELSE 'Supporting Character'
  END AS narrative_role,
  
  -- Character alignment (light side/dark side)
  CASE 
    WHEN cfe.character_name IN ('Darth Vader', 'Emperor Palpatine', 'Darth Maul',
                            'Count Dooku', 'General Grievous', 'Kylo Ren',
                            'Captain Phasma', 'General Hux', 'Moff Tarkin',
                            'Jabba the Hutt', 'Boba Fett', 'Jango Fett') 
      THEN 'Villain'
    WHEN cfe.character_name IN ('Luke Skywalker', 'Leia Organa', 'Han Solo',
                            'Obi-Wan Kenobi', 'Yoda', 'Rey', 'Finn', 'Poe Dameron',
                            'Padmé Amidala', 'Qui-Gon Jinn', 'Mace Windu') 
      THEN 'Hero'
    WHEN cfe.character_name IN ('Lando Calrissian', 'Anakin Skywalker') 
      THEN 'Ambiguous'
    ELSE 'Neutral'
  END AS character_alignment,
  
  -- Film significance
  CASE
    WHEN cfe.character_importance_tier = 'Protagonist/Antagonist' AND
         ((cfe.character_name = 'Luke Skywalker' AND cfe.episode_id IN (4, 5, 6)) OR
          (cfe.character_name = 'Anakin Skywalker' AND cfe.episode_id IN (1, 2, 3)) OR
          (cfe.character_name = 'Darth Vader' AND cfe.episode_id IN (4, 5, 6)) OR
          (cfe.character_name = 'Rey' AND cfe.episode_id IN (7, 8, 9)) OR
          (cfe.character_name = 'Kylo Ren' AND cfe.episode_id IN (7, 8, 9))) 
      THEN 'Pivotal'
    WHEN cfe.character_importance_tier = 'Protagonist/Antagonist' 
      THEN 'Crucial'
    WHEN cfe.character_importance_tier = 'Major' 
      THEN 'Significant'
    WHEN cfe.character_importance_tier = 'Supporting' 
      THEN 'Important'
    ELSE 'Background'
  END AS film_significance,
  
  -- Time Dimension
  CURRENT_TIMESTAMP AS dbt_loaded_at
  
FROM character_film_enriched cfe
ORDER BY 
  COALESCE(cfe.episode_id, 999),
  cfe.character_importance_tier,
  cfe.character_name
  );
  