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
  
  FIXED - Now using the correct relationship between films and characters
  - Films contain arrays of character IDs in the characters field
*/

WITH film_characters AS (
    -- Extract character references from the films data
    SELECT
        f.id AS film_id,
        f.film_title,
        f.episode_id,
        f.release_date,
        f.director,
        -- Add trilogy categorization based on episode_id
        CASE
            WHEN f.episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
            WHEN f.episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy'
            WHEN f.episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
            ELSE 'Anthology Films'
        END AS trilogy,
        -- Convert character reference to integer (it's just a plain number from jsonb_array_elements_text)
        NULLIF(character_ref, '')::INTEGER AS character_id
    FROM {{ ref('stg_swapi_films') }} f,
    LATERAL jsonb_array_elements_text(
        CASE WHEN f.characters IS NULL OR f.characters = 'null' 
             THEN '[]'::jsonb
             ELSE f.characters END
    ) WITH ORDINALITY AS chars(character_ref, idx)
    WHERE f.id IS NOT NULL
),

-- Join with character information
character_details AS (
    SELECT
        fc.film_id,
        fc.film_title,
        fc.episode_id,
        fc.release_date,
        fc.director,
        fc.trilogy,
        fc.character_id,
        p.name AS character_name,
        p.gender,
        p.birth_year,
        p.homeworld_id,
        -- Calculate or derive fields not directly available
        EXTRACT(YEAR FROM fc.release_date) AS release_year,
        COALESCE(fc.episode_id, 999) AS chronological_order
    FROM film_characters fc
    LEFT JOIN {{ ref('stg_swapi_people') }} p ON fc.character_id = p.id
),

-- Character importance tiers
character_importance AS (
    SELECT 
        cd.*,
        CASE
            -- Protagonist/Antagonist tier - central characters
            WHEN cd.character_name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 
                                      'Rey', 'Kylo Ren', 'Din Djarin', 'Grogu') 
                THEN 'Protagonist/Antagonist'
            
            -- Major characters
            WHEN cd.character_name IN ('Han Solo', 'Leia Organa', 'Obi-Wan Kenobi', 
                                      'Emperor Palpatine', 'Finn', 'Poe Dameron',
                                      'Padmé Amidala', 'Count Dooku', 'Qui-Gon Jinn',
                                      'Darth Maul', 'Jyn Erso', 'Cassian Andor', 'Boba Fett') 
                THEN 'Major'
            
            -- Supporting characters
            WHEN cd.character_name IN ('Chewbacca', 'C-3PO', 'R2-D2', 'Yoda', 
                                      'Lando Calrissian', 'Mace Windu', 'General Grievous', 
                                      'Admiral Ackbar', 'BB-8', 'General Hux', 'Rose Tico',
                                      'Moff Gideon', 'Saw Gerrera', 'Jabba the Hutt',
                                      'Ahsoka Tano') 
                THEN 'Supporting'
                                      
            -- Additional major characters by ID
            WHEN cd.character_id IN (1, 2, 3, 4, 5, 10, 11, 13) 
                THEN 'Major'
            
            -- Characters with more details are likely more important
            WHEN cd.gender IS NOT NULL AND cd.birth_year IS NOT NULL 
                THEN 'Notable'
            
            ELSE 'Minor'
        END AS character_importance_tier
    FROM character_details cd
),

-- Apply film saga classification
film_saga AS (
    SELECT
        ci.*,
        ci.trilogy AS film_saga,
        CASE
            WHEN ci.character_importance_tier = 'Protagonist/Antagonist' THEN 1
            WHEN ci.character_importance_tier = 'Major' THEN 2
            WHEN ci.character_importance_tier = 'Supporting' THEN 3
            WHEN ci.character_importance_tier = 'Notable' THEN 4
            ELSE 5
        END AS importance_rank,
        -- Count characters per film to calculate metrics
        COUNT(*) OVER (PARTITION BY ci.film_id) AS character_count,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY ci.film_id) <= 10 THEN 'Small Cast'
            WHEN COUNT(*) OVER (PARTITION BY ci.film_id) <= 25 THEN 'Medium Cast'
            ELSE 'Large Cast'
        END AS cast_size_category
    FROM character_importance ci
)

-- Final selection with all metrics
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
    
    -- Character role classification
    fs.character_importance_tier AS character_role,
    
    -- Film categorization
    fs.trilogy AS film_saga,
    fs.release_year,
    fs.character_count AS total_character_count,
    fs.cast_size_category,
    
    -- Character's homeworld
    fs.homeworld_id,
    
    -- Character significance metrics
    fs.importance_rank,
    
    -- Calculate appearance metrics
    ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.chronological_order
    ) AS chronological_appearance_number,
    
    ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.release_date
    ) AS release_order_appearance_number,
    
    -- Character appearance percentage
    (COUNT(fs.character_id) OVER (PARTITION BY fs.character_id))::FLOAT / 
    (SELECT COUNT(DISTINCT id) FROM {{ ref('stg_swapi_films') }})::FLOAT * 100 AS saga_appearance_percentage,
    
    -- Timeline attributes
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.chronological_order
    ) = 1 THEN TRUE ELSE FALSE END AS is_first_chronological_appearance,
    
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.release_date
    ) = 1 THEN TRUE ELSE FALSE END AS is_first_release_appearance,
    
    -- Narrative role in specific films
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
    
    -- Character alignment (light side/dark side)
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
    
    -- Film significance
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
ORDER BY COALESCE(fs.episode_id, 999), fs.character_importance_tier, fs.character_name