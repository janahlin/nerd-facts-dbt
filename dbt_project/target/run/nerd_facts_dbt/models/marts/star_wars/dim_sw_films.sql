
  
    

  create  table "nerd_facts"."public"."dim_sw_films__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: dim_sw_films
  Description: Film dimension table with enriched attributes
*/

WITH film_base AS (
  SELECT
    f.film_id,
    f.title,
    f.episode_id,
    f.opening_crawl,
    f.director,
    f.producer,
    f.release_date
  FROM "nerd_facts"."public"."int_swapi_films" f
),

-- Character counts
character_counts AS (
  SELECT
    fc.film_id,
    COUNT(DISTINCT fc.people_id) AS character_count
  FROM "nerd_facts"."public"."int_swapi_films_characters" fc
  GROUP BY fc.film_id
),

-- Planet counts
planet_counts AS (
  SELECT
    fp.film_id,
    COUNT(DISTINCT fp.planet_id) AS planet_count
  FROM "nerd_facts"."public"."int_swapi_films_planets" fp
  GROUP BY fp.film_id
),

-- Vehicle counts
vehicle_counts AS (
  SELECT
    fv.film_id,
    COUNT(DISTINCT fv.vehicle_id) AS vehicle_count
  FROM "nerd_facts"."public"."int_swapi_films_vehicles" fv
  GROUP BY fv.film_id
),

-- Starship counts
starship_counts AS (
  SELECT
    fs.film_id,
    COUNT(DISTINCT fs.starship_id) AS starship_count
  FROM "nerd_facts"."public"."int_swapi_films_starships" fs
  GROUP BY fs.film_id
),

-- Species counts
species_counts AS (
  SELECT
    fsp.film_id,
    COUNT(DISTINCT fsp.species_id) AS species_count
  FROM "nerd_facts"."public"."int_swapi_films_species" fsp
  GROUP BY fsp.film_id
)

SELECT
  -- Primary Key
  md5(cast(coalesce(cast(fb.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS film_key,
  
  -- Natural Key
  fb.film_id,
  
  -- Film Attributes
  fb.title AS film_title,
  fb.episode_id,
  fb.opening_crawl,
  fb.director,
  fb.producer,
  fb.release_date,
  
  -- Film Classification
  CASE
    WHEN fb.episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
    WHEN fb.episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy'
    WHEN fb.episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
    ELSE 'Anthology Films'
  END AS film_saga,
  
  CASE
    WHEN fb.episode_id BETWEEN 1 AND 3 THEN 'Republic Era'
    WHEN fb.episode_id BETWEEN 4 AND 6 THEN 'Imperial Era'
    WHEN fb.episode_id BETWEEN 7 AND 9 THEN 'New Republic Era'
    ELSE 'Various'
  END AS era,
  
  -- Release information
  EXTRACT(YEAR FROM fb.release_date) AS release_year,
  
  -- Entity counts
  COALESCE(cc.character_count, 0) AS character_count,
  COALESCE(pc.planet_count, 0) AS planet_count,
  COALESCE(vc.vehicle_count, 0) AS vehicle_count,
  COALESCE(sc.starship_count, 0) AS starship_count,
  COALESCE(spc.species_count, 0) AS species_count,
  
  -- Calculated metrics
  COALESCE(cc.character_count, 0) + 
  COALESCE(pc.planet_count, 0) + 
  COALESCE(vc.vehicle_count, 0) + 
  COALESCE(sc.starship_count, 0) + 
  COALESCE(spc.species_count, 0) AS total_entity_count,
  
  -- Cast size classification
  CASE
    WHEN COALESCE(cc.character_count, 0) <= 10 THEN 'Small Cast'
    WHEN COALESCE(cc.character_count, 0) <= 25 THEN 'Medium Cast'
    ELSE 'Large Cast'
  END AS cast_size_category,
  
  -- Film importance
  CASE
    WHEN fb.episode_id IN (4, 5, 6) THEN 'Foundational'
    WHEN fb.episode_id IN (1, 2, 3, 7, 8, 9) THEN 'Main Saga'
    ELSE 'Extended Universe'
  END AS film_importance,
  
  -- Time Dimension
  CURRENT_TIMESTAMP AS dbt_loaded_at

FROM film_base fb
LEFT JOIN character_counts cc ON fb.film_id = cc.film_id
LEFT JOIN planet_counts pc ON fb.film_id = pc.film_id
LEFT JOIN vehicle_counts vc ON fb.film_id = vc.film_id
LEFT JOIN starship_counts sc ON fb.film_id = sc.film_id
LEFT JOIN species_counts spc ON fb.film_id = spc.film_id
ORDER BY fb.episode_id
  );
  