/*
  Model: stg_swapi_films
  Description: Standardizes Star Wars film data from SWAPI
  Source: raw.swapi_films
  
  Notes:
  - Adds derived fields for easier analysis (release_year, trilogy)
  - Extracts counts from related entities (characters, planets, species)
  - Converts dates to proper DATE types and adds chronological ordering
*/

WITH raw_data AS (
    -- Explicitly list columns to prevent issues if source schema changes
    SELECT
        id,
        episode_id,
        title,
        director,
        producer,
        release_date,
        opening_crawl,
        characters,
        planets,
        starships,
        vehicles,
        species,
        created,
        edited,
        url
    FROM raw.swapi_films
    WHERE id IS NOT NULL
)

SELECT
    -- Primary identifiers
    id,
    episode_id,
    title AS film_title,
    
    -- Creative team
    director,
    producer,
    
    -- Standardize dates and add derived time fields
    CASE 
        WHEN release_date IS NULL OR release_date = '' THEN NULL
        ELSE TO_DATE(release_date, 'YYYY-MM-DD') 
    END AS release_date,
    
    EXTRACT(YEAR FROM TO_DATE(release_date, 'YYYY-MM-DD')) AS release_year,
    
    -- Story elements
    opening_crawl,
    
    -- Derived chronological order (story timeline vs. release order)
    CASE episode_id
        WHEN 1 THEN 1  -- The Phantom Menace
        WHEN 2 THEN 2  -- Attack of the Clones
        WHEN 3 THEN 3  -- Revenge of the Sith
        WHEN 4 THEN 4  -- A New Hope
        WHEN 5 THEN 5  -- Empire Strikes Back
        WHEN 6 THEN 6  -- Return of the Jedi
        WHEN 7 THEN 7  -- The Force Awakens
        WHEN 8 THEN 8  -- The Last Jedi
        WHEN 9 THEN 9  -- The Rise of Skywalker
        ELSE 99        -- Unknown/other
    END AS chronological_order,
    
    -- Trilogy classification
    CASE
        WHEN episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
        WHEN episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy'
        WHEN episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
        ELSE 'Anthology'
    END AS trilogy,
    
    -- Entity counts
    COALESCE(jsonb_array_length(characters), 0) AS character_count,
    COALESCE(jsonb_array_length(planets), 0) AS planet_count,
    COALESCE(jsonb_array_length(starships), 0) AS starship_count,
    COALESCE(jsonb_array_length(vehicles), 0) AS vehicle_count,
    COALESCE(jsonb_array_length(species), 0) AS species_count,
    
    -- Word count of opening crawl (approximate)
    ARRAY_LENGTH(STRING_TO_ARRAY(REGEXP_REPLACE(opening_crawl, '\r|\n', ' ', 'g'), ' '), 1) AS opening_crawl_word_count,
    
    -- API metadata with proper handling
    created::TIMESTAMP AS created_at,
    edited::TIMESTAMP AS updated_at,
    
    -- Data tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM raw_data
ORDER BY episode_id
