
  
    

  create  table "nerd_facts"."public"."dim_sw_species__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: dim_sw_species
  Description: Species dimension table with enriched attributes and classifications
*/

WITH species_base AS (
    SELECT
        s.species_id,
        s.species_name,  -- Using species_name directly instead of s.name
        s.classification,
        s.designation,
        s.average_height,
        s.skin_colors,
        s.hair_colors,
        s.eye_colors,
        s.average_lifespan,
        s.language,
        s.homeworld AS homeworld_id  -- Use homeworld instead of homeworld_id
    FROM "nerd_facts"."public"."int_swapi_species" s
),

-- Film appearances
film_appearances AS (
    SELECT
        fs.species_id,
        COUNT(DISTINCT fs.film_id) AS film_count
    FROM "nerd_facts"."public"."int_swapi_films_species" fs
    GROUP BY fs.species_id
),

-- Character counts - estimate based on species mentions in films
-- Since we don't have direct access to character-species relationships
character_counts AS (
    SELECT
        fs.species_id,
        COUNT(DISTINCT fc.people_id) AS character_count
    FROM "nerd_facts"."public"."int_swapi_films_species" fs
    JOIN "nerd_facts"."public"."int_swapi_films_characters" fc ON fs.film_id = fc.film_id
    GROUP BY fs.species_id
),

-- Enriched species data
species_enriched AS (
    SELECT
        sb.*,
        COALESCE(fa.film_count, 0) AS film_count,
        COALESCE(cc.character_count, 0) AS character_count
    FROM species_base sb
    LEFT JOIN film_appearances fa ON sb.species_id = fa.species_id
    LEFT JOIN character_counts cc ON sb.species_id = cc.species_id
)

SELECT
    -- Primary Key
    md5(cast(coalesce(cast(se.species_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS species_key,
    
    -- Natural Key
    se.species_id,
    
    -- Species Attributes
    se.species_name,
    se.classification,
    se.designation,
    se.average_height,
    se.skin_colors,
    se.hair_colors,
    se.eye_colors,
    se.average_lifespan,
    se.language,
    se.homeworld_id,
    
    -- Related Dimensions
    md5(cast(coalesce(cast(se.homeworld_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS homeworld_key,
    
    -- Appearance Metrics
    se.film_count,
    se.character_count,
    
    -- Species Classifications
    CASE
        WHEN se.classification ILIKE '%mammal%' THEN 'Mammalian'
        WHEN se.classification ILIKE '%reptile%' THEN 'Reptilian'
        WHEN se.classification ILIKE '%amphibian%' THEN 'Amphibian'
        WHEN se.classification ILIKE '%insect%' THEN 'Insectoid'
        WHEN se.classification ILIKE '%sentient%' THEN 'Sentient'
        ELSE se.classification
    END AS species_type,
    
    -- Sentience level
    CASE
        WHEN se.designation ILIKE '%sentient%' THEN 'Sentient'
        ELSE 'Non-sentient'
    END AS sentience_level,
    
    -- Size classification based on average_height
    CASE
        WHEN se.average_height::NUMERIC > 200 THEN 'Tall'
        WHEN se.average_height::NUMERIC BETWEEN 150 AND 200 THEN 'Medium'
        WHEN se.average_height::NUMERIC > 0 AND se.average_height::NUMERIC < 150 THEN 'Short'
        ELSE 'Unknown'
    END AS size_classification,
    
    -- Longevity based on average_lifespan
    CASE
        WHEN se.average_lifespan::NUMERIC > 500 THEN 'Very Long-Lived'
        WHEN se.average_lifespan::NUMERIC > 150 THEN 'Long-Lived'
        WHEN se.average_lifespan::NUMERIC > 70 THEN 'Standard'
        WHEN se.average_lifespan::NUMERIC > 0 THEN 'Short-Lived'
        ELSE 'Unknown'
    END AS longevity_classification,
    
    -- Notable Species flag
    CASE 
        WHEN se.species_name IN ('Human', 'Wookiee', 'Droid', 'Hutt', 'Yoda''s species', 
                              'Zabrak', 'Twi''lek', 'Mon Calamari', 'Ewok', 'Gungan') 
            OR se.film_count >= 3  
        THEN TRUE
        ELSE FALSE
    END AS is_notable_species,
    
    -- Force sensitivity prevalence
    CASE 
        WHEN se.species_name IN ('Yoda''s species') THEN 'High'
        WHEN se.species_name IN ('Human', 'Zabrak', 'Togruta') THEN 'Medium'
        ELSE 'Low/Unknown'
    END AS force_sensitivity_prevalence,
    
    -- Narrative importance
    CASE
        WHEN se.film_count >= 3 THEN 'Major'
        WHEN se.film_count >= 2 THEN 'Significant'
        WHEN se.film_count = 1 THEN 'Featured'
        ELSE 'Minor'
    END AS narrative_importance,
    
    -- Data Tracking
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM species_enriched se
  );
  