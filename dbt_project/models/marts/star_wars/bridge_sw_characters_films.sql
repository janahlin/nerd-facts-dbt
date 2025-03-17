{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['character_id']}, {'columns': ['film_id']}, {'columns': ['character_film_id']}],
    unique_key = 'character_film_id'
  )
}}

/*
  Model: bridge_sw_characters_films
  Description: Bridge table connecting Star Wars characters to the films they appear in
  
  Notes:
  - Handles the many-to-many relationship between characters and films
  - Extracts film references from the nested arrays in character data
  - Calculates character importance and appearance sequences
  - Provides context for character arcs across the film timeline
  - Adds derived attributes about character relationships to each film
*/

WITH character_films AS (
    -- Extract film references from the characters data with improved error handling
    SELECT
        p.id AS character_id,
        p.name AS character_name,
        p.gender,
        p.birth_year,
        p.species_id,
        film_ref->>'url' AS film_url,
        -- Extract film ID from URL with better error handling
        NULLIF(SPLIT_PART(COALESCE(film_ref->>'url', ''), '/', 6), '')::INTEGER AS film_id
    FROM {{ ref('stg_swapi_people') }} p,
    LATERAL jsonb_array_elements(
        CASE WHEN p.films IS NULL OR p.films = 'null' THEN '[]'::jsonb
        ELSE p.films END
    ) AS film_ref
    WHERE p.id IS NOT NULL
),

-- Join with film information for additional context
film_details AS (
    SELECT
        cf.character_id,
        cf.character_name,
        cf.gender,
        cf.birth_year,
        cf.species_id,
        cf.film_id,
        f.title AS film_title,
        f.episode_id,
        f.release_date,
        f.director
    FROM character_films cf
    LEFT JOIN {{ ref('stg_swapi_films') }} f ON cf.film_id = f.id
),

-- Character importance tiers with expanded classifications
character_importance AS (
    SELECT 
        fd.*,
        CASE
            -- Protagonist/Antagonist tier - the most central characters
            WHEN fd.character_name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 
                                      'Rey', 'Kylo Ren', 'Din Djarin', 'Grogu') THEN 'Protagonist/Antagonist'
            
            -- Major characters - very important to the plot but not the absolute center
            WHEN fd.character_name IN ('Han Solo', 'Leia Organa', 'Obi-Wan Kenobi', 'Emperor Palpatine',
                                      'Finn', 'Poe Dameron', 'Padmé Amidala', 'Count Dooku', 'Qui-Gon Jinn',
                                      'Darth Maul', 'Jyn Erso', 'Cassian Andor', 'Boba Fett') THEN 'Major'
            
            -- Supporting characters - recognizable and important secondary characters
            WHEN fd.character_name IN ('Chewbacca', 'C-3PO', 'R2-D2', 'Yoda', 'Lando Calrissian',
                                      'Mace Windu', 'General Grievous', 'Admiral Ackbar', 'BB-8',
                                      'General Hux', 'Rose Tico', 'Moff Gideon', 'Saw Gerrera',
                                      'Jabba the Hutt', 'Ahsoka Tano') THEN 'Supporting'
                                      
            -- Determine recursively based on metadata
            WHEN fd.character_id IN (1, 2, 3, 4, 5, 10, 11, 13) THEN 'Major' -- Additional known major characters by ID
            
            -- Use gender and birth_year to help detect likely important characters
            WHEN fd.gender IS NOT NULL AND fd.birth_year IS NOT NULL THEN 'Notable'
            
            -- Default case
            ELSE 'Minor'
        END AS character_importance_tier
    FROM film_details fd
),

-- Add film saga classification
film_saga AS (
    SELECT
        ci.*,
        CASE
            WHEN ci.episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
            WHEN ci.episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy'
            WHEN ci.episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
            WHEN ci.title = 'Rogue One' THEN 'Anthology'
            WHEN ci.title = 'Solo' THEN 'Anthology'
            ELSE 'Other'
        END AS film_saga
    FROM character_importance ci
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['fs.character_id', 'fs.film_id']) }} AS character_film_id,
    
    -- Foreign keys to related dimensions
    {{ dbt_utils.generate_surrogate_key(['fs.character_id']) }} AS character_key,
    {{ dbt_utils.generate_surrogate_key(['fs.film_id']) }} AS film_key,
    
    -- Core identifiers
    fs.character_id,
    fs.character_name,
    fs.film_id,
    fs.film_title,
    fs.episode_id,
    
    -- Enhanced character role classification with more nuance
    fs.character_importance_tier AS character_role,
    
    -- Film saga categorization
    fs.film_saga,
    
    -- Character's species relationship
    fs.species_id,
    
    -- Calculate appearance metrics
    ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.episode_id
    ) AS chronological_appearance_number,
    
    ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.release_date
    ) AS release_order_appearance_number,
    
    -- Timeline attributes
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.episode_id
    ) = 1 THEN TRUE ELSE FALSE END AS is_first_chronological_appearance,
    
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.release_date
    ) = 1 THEN TRUE ELSE FALSE END AS is_first_release_appearance,
    
    CASE WHEN fs.character_name IN ('Darth Vader', 'Anakin Skywalker') AND 
              fs.episode_id BETWEEN 1 AND 3 THEN 'Protagonist'
         WHEN fs.character_name IN ('Darth Vader') AND 
              fs.episode_id BETWEEN 4 AND 6 THEN 'Antagonist'
         WHEN fs.character_name = 'Luke Skywalker' AND 
              fs.episode_id BETWEEN 4 AND 6 THEN 'Protagonist'
         WHEN fs.character_name = 'Rey' AND 
              fs.episode_id BETWEEN 7 AND 9 THEN 'Protagonist'
         WHEN fs.character_name = 'Kylo Ren' AND 
              fs.episode_id BETWEEN 7 AND 8 THEN 'Antagonist'
         WHEN fs.character_name = 'Kylo Ren' AND 
              fs.episode_id = 9 THEN 'Protagonist'
         WHEN fs.character_importance_tier = 'Protagonist/Antagonist' THEN 'Key Character'
         WHEN fs.character_importance_tier = 'Major' THEN 'Major Character'
         ELSE 'Supporting Character'
    END AS narrative_role,
    
    -- Light side/dark side alignment
    CASE 
        WHEN fs.character_name IN ('Darth Vader', 'Emperor Palpatine', 'Darth Maul',
                                'Count Dooku', 'General Grievous', 'Kylo Ren',
                                'Captain Phasma', 'General Hux', 'Moff Tarkin',
                                'Jabba the Hutt', 'Boba Fett', 'Jango Fett') THEN 'Villain'
        WHEN fs.character_name IN ('Luke Skywalker', 'Leia Organa', 'Han Solo',
                                'Obi-Wan Kenobi', 'Yoda', 'Rey', 'Finn', 'Poe Dameron',
                                'Padmé Amidala', 'Qui-Gon Jinn', 'Mace Windu') THEN 'Hero'
        WHEN fs.character_name IN ('Lando Calrissian', 'Anakin Skywalker') THEN 'Ambiguous'
        ELSE 'Neutral'
    END AS character_alignment,
    
    -- Film significance - how crucial the character is to this specific film
    CASE
        WHEN fs.character_importance_tier = 'Protagonist/Antagonist' AND
             ((fs.character_name = 'Luke Skywalker' AND fs.episode_id IN (4, 5, 6)) OR
              (fs.character_name = 'Anakin Skywalker' AND fs.episode_id IN (1, 2, 3)) OR
              (fs.character_name = 'Darth Vader' AND fs.episode_id IN (4, 5, 6)) OR
              (fs.character_name = 'Rey' AND fs.episode_id IN (7, 8, 9)) OR
              (fs.character_name = 'Kylo Ren' AND fs.episode_id IN (7, 8, 9))) THEN 'Pivotal'
        WHEN fs.character_importance_tier = 'Protagonist/Antagonist' THEN 'Crucial'
        WHEN fs.character_importance_tier = 'Major' THEN 'Significant'
        WHEN fs.character_importance_tier = 'Supporting' THEN 'Important'
        ELSE 'Background'
    END AS film_significance,
    
    -- Meta information
    fs.release_date,
    fs.director,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM film_saga fs
ORDER BY fs.episode_id, character_role, fs.character_name