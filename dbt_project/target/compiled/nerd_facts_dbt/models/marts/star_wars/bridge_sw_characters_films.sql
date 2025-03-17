

/*
  Model: bridge_sw_characters_films
  Description: Bridge table connecting Star Wars characters to the films they appear in
  
  Notes:
  - Handles the many-to-many relationship between characters and films
  - Extracts film references from the nested arrays in character data
  - Calculates character importance and appearance sequences
  - Adapted to work with our current staging model structure
*/

WITH character_films AS (
    -- Extract film references from the characters data with improved error handling
    SELECT
        p.id AS character_id,
        p.name AS character_name,
        p.gender,
        p.birth_year,
        -- Using NULL for species_id since we don't have it in staging yet
        NULL AS species_id,
        p.homeworld_id,
        film_ref->>'url' AS film_url,
        -- Extract film ID from URL with better error handling
        NULLIF(SPLIT_PART(COALESCE(film_ref->>'url', ''), '/', 6), '')::INTEGER AS film_id
    FROM "nerd_facts"."public"."stg_swapi_people" p,
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
        cf.homeworld_id,
        cf.film_id,
        f.film_title,
        f.episode_id,
        f.release_date,
        f.director,
        -- Calculate or derive fields not present in staging
        EXTRACT(YEAR FROM f.release_date) AS release_year,
        f.trilogy,                  
        COALESCE(f.episode_id, 999) AS chronological_order,  -- Default high value for non-episode films
        -- Count words in opening_crawl
        COALESCE(ARRAY_LENGTH(STRING_TO_ARRAY(f.opening_crawl, ' '), 1), 0) AS opening_crawl_word_count,
        f.character_count,
        f.planet_count,
        f.starship_count,
        f.vehicle_count,
        f.species_count,
        -- Include ETL tracking fields
        f.url AS film_url,
        NULL AS fetch_timestamp,
        NULL AS processed_timestamp
    FROM character_films cf
    LEFT JOIN "nerd_facts"."public"."stg_swapi_films" f ON cf.film_id = f.id
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

-- Add film saga classification - use trilogy field directly and enhance
film_saga AS (
    SELECT
        ci.*,
        -- Use the trilogy field directly from staging instead of recalculating
        ci.trilogy AS film_saga,
        -- Add enhanced fields for better analytics
        CASE
            WHEN ci.character_count <= 10 THEN 'Small Cast'
            WHEN ci.character_count <= 25 THEN 'Medium Cast'
            ELSE 'Large Cast'
        END AS cast_size_category,
        -- Calculate character's relative prominence in film
        CASE
            WHEN ci.character_importance_tier = 'Protagonist/Antagonist' THEN 1
            WHEN ci.character_importance_tier = 'Major' THEN 2
            WHEN ci.character_importance_tier = 'Supporting' THEN 3
            WHEN ci.character_importance_tier = 'Notable' THEN 4
            ELSE 5
        END AS importance_rank
    FROM character_importance ci
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(fs.character_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fs.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_film_id,
    
    -- Foreign keys to related dimensions
    md5(cast(coalesce(cast(fs.character_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS character_key,
    md5(cast(coalesce(cast(fs.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS film_key,
    
    -- Core identifiers
    fs.character_id,
    fs.character_name,
    fs.film_id,
    fs.film_title,
    fs.episode_id,
    
    -- Enhanced character role classification with more nuance
    fs.character_importance_tier AS character_role,
    
    -- Film saga categorization - use trilogy field directly
    fs.trilogy AS film_saga,
    fs.release_year,
    COALESCE(fs.character_count, 0) AS total_character_count,
    fs.cast_size_category,
    
    -- Character's species relationship
    fs.species_id,
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
    (SELECT COUNT(DISTINCT id) FROM "nerd_facts"."public"."stg_swapi_films")::FLOAT * 100 AS saga_appearance_percentage,
    
    -- Timeline attributes
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY fs.character_id 
        ORDER BY fs.chronological_order
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
    
    -- Film metrics (derived)
    CASE
        -- More granular logic with percentage of lines/screentime using character count
        WHEN COALESCE(fs.character_count, 0) < 15 AND fs.character_importance_tier IN ('Protagonist/Antagonist', 'Major') 
            THEN 'High Focus'
        WHEN COALESCE(fs.character_count, 0) > 30 AND fs.character_importance_tier = 'Minor'
            THEN 'Background Character'
        ELSE 'Standard Focus'
    END AS character_screen_focus,
    
    -- Meta information
    fs.release_date,
    fs.director,
    
    -- Source tracking
    fs.film_url,
    fs.fetch_timestamp,
    fs.processed_timestamp,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM film_saga fs
ORDER BY COALESCE(fs.episode_id, 999), character_role, fs.character_name