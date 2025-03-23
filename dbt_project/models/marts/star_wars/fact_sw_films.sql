{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['film_key']}],
    unique_key = 'film_key'
  )
}}

/*
  Model: fact_sw_films
  Description: Film fact table with metrics about characters, planets, vehicles, starships and species
*/

-- Film base information
WITH film_base AS (
  SELECT
    f.film_id,
    f.title,
    f.episode_id,
    f.release_date,
    EXTRACT(YEAR FROM f.release_date) AS release_year
  FROM {{ ref('int_swapi_films') }} f
),

-- Character counts with gender breakdown
character_metrics AS (
  SELECT 
    fc.film_id,
    COUNT(DISTINCT fc.people_id) AS total_characters,
    SUM(CASE WHEN p.gender = 'male' THEN 1 ELSE 0 END) AS male_characters,
    SUM(CASE WHEN p.gender = 'female' THEN 1 ELSE 0 END) AS female_characters,
    SUM(CASE WHEN p.gender NOT IN ('male', 'female') OR p.gender IS NULL THEN 1 ELSE 0 END) AS other_gender_characters,
    -- Force users
    SUM(CASE 
      WHEN p.name IN ('Luke Skywalker', 'Darth Vader', 'Anakin Skywalker', 'Rey', 'Kylo Ren',
                    'Obi-Wan Kenobi', 'Emperor Palpatine', 'Yoda', 'Mace Windu',
                    'Count Dooku', 'Qui-Gon Jinn', 'Darth Maul', 'Ahsoka Tano')
      THEN 1 ELSE 0 
    END) AS force_users
  FROM {{ ref('int_swapi_films_characters') }} fc
  JOIN {{ ref('int_swapi_people') }} p ON fc.people_id = p.people_id
  GROUP BY fc.film_id
),

-- Planet counts
planet_metrics AS (
  SELECT 
    fp.film_id,
    COUNT(DISTINCT fp.planet_id) AS total_planets
  FROM {{ ref('int_swapi_films_planets') }} fp
  GROUP BY fp.film_id
),

-- Vehicle counts
vehicle_metrics AS (
  SELECT 
    fv.film_id,
    COUNT(DISTINCT fv.vehicle_id) AS total_vehicles
  FROM {{ ref('int_swapi_films_vehicles') }} fv
  GROUP BY fv.film_id
),

-- Starship counts
starship_metrics AS (
  SELECT 
    fs.film_id,
    COUNT(DISTINCT fs.starship_id) AS total_starships
  FROM {{ ref('int_swapi_films_starships') }} fs
  GROUP BY fs.film_id
),

-- Species counts
species_metrics AS (
  SELECT 
    fsp.film_id,
    COUNT(DISTINCT fsp.species_id) AS total_species
  FROM {{ ref('int_swapi_films_species') }} fsp
  GROUP BY fsp.film_id
)

SELECT
  -- Primary Key (same as dim_films)
  {{ dbt_utils.generate_surrogate_key(['fb.film_id']) }} AS film_key,
  
  -- Natural Key
  fb.film_id,
  
  -- Film Identifiers
  fb.title AS film_title,
  fb.episode_id,
  fb.release_date,
  fb.release_year,
  
  -- Character Metrics
  COALESCE(cm.total_characters, 0) AS character_count,
  COALESCE(cm.male_characters, 0) AS male_character_count,
  COALESCE(cm.female_characters, 0) AS female_character_count,
  COALESCE(cm.other_gender_characters, 0) AS other_gender_character_count,
  COALESCE(cm.force_users, 0) AS force_user_count,
  
  -- Gender diversity ratio (percentage of non-male characters)
  CASE 
    WHEN COALESCE(cm.total_characters, 0) > 0 
    THEN ROUND((COALESCE(cm.female_characters, 0) * 100.0 / COALESCE(cm.total_characters, 1)), 1)
    ELSE 0
  END AS female_character_percentage,
  
  -- Force user ratio
  CASE 
    WHEN COALESCE(cm.total_characters, 0) > 0 
    THEN ROUND((COALESCE(cm.force_users, 0) * 100.0 / COALESCE(cm.total_characters, 1)), 1)
    ELSE 0
  END AS force_user_percentage,
  
  -- Planet Metrics
  COALESCE(pm.total_planets, 0) AS planet_count,
  
  -- Vehicle Metrics
  COALESCE(vm.total_vehicles, 0) AS vehicle_count,
  
  -- Starship Metrics
  COALESCE(sm.total_starships, 0) AS starship_count,
  
  -- Species Metrics
  COALESCE(spm.total_species, 0) AS species_count,
  
  -- Total Entity Count
  (COALESCE(cm.total_characters, 0) + COALESCE(pm.total_planets, 0) + 
   COALESCE(vm.total_vehicles, 0) + COALESCE(sm.total_starships, 0) +
   COALESCE(spm.total_species, 0)) AS total_entity_count,
  
  -- Vehicle to Character Ratio
  CASE 
    WHEN COALESCE(cm.total_characters, 0) > 0
    THEN ROUND(((COALESCE(vm.total_vehicles, 0) + COALESCE(sm.total_starships, 0)) * 1.0 / 
           NULLIF(COALESCE(cm.total_characters, 0), 0)), 2)
    ELSE 0
  END AS vehicle_character_ratio,
  
  -- Time Dimension
  CURRENT_TIMESTAMP AS dbt_loaded_at

FROM film_base fb
LEFT JOIN character_metrics cm ON fb.film_id = cm.film_id
LEFT JOIN planet_metrics pm ON fb.film_id = pm.film_id
LEFT JOIN vehicle_metrics vm ON fb.film_id = vm.film_id
LEFT JOIN starship_metrics sm ON fb.film_id = sm.film_id
LEFT JOIN species_metrics spm ON fb.film_id = spm.film_id
ORDER BY fb.episode_id