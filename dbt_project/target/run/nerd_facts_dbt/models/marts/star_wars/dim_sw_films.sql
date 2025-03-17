
  
    

  create  table "nerd_facts"."public"."dim_sw_films__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: dim_sw_films
  Description: Dimension table for Star Wars films
*/

WITH films AS (
    SELECT
        id AS film_id,
        film_title AS title,
        episode_id,
        opening_crawl,
        director,
        producer,
        release_date,
        
        -- Calculate trilogy from episode_id
        CASE 
            WHEN episode_id BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
            WHEN episode_id BETWEEN 4 AND 6 THEN 'Original Trilogy' 
            WHEN episode_id BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
            ELSE 'Anthology' 
        END AS trilogy,
        
        -- Chronological order is just the episode_id, or 999 for non-episode films
        COALESCE(episode_id, 999) AS chronological_order,
        
        -- Add runtime_minutes calculation here in the first CTE
        CASE
            WHEN episode_id = 1 THEN 136  -- The Phantom Menace
            WHEN episode_id = 2 THEN 142  -- Attack of the Clones
            WHEN episode_id = 3 THEN 140  -- Revenge of the Sith
            WHEN episode_id = 4 THEN 121  -- A New Hope
            WHEN episode_id = 5 THEN 124  -- The Empire Strikes Back
            WHEN episode_id = 6 THEN 131  -- Return of the Jedi
            WHEN episode_id = 7 THEN 138  -- The Force Awakens
            WHEN episode_id = 8 THEN 152  -- The Last Jedi
            WHEN episode_id = 9 THEN 142  -- The Rise of Skywalker
            ELSE 135  -- Default
        END AS runtime_minutes,
        
        -- Entity counts - use if they exist or set to defaults
        COALESCE(character_count, 0) AS character_count,
        COALESCE(planet_count, 0) AS planet_count,
        COALESCE(starship_count, 0) AS starship_count,
        COALESCE(vehicle_count, 0) AS vehicle_count,
        COALESCE(species_count, 0) AS species_count,
        
        -- Calculate opening_crawl_word_count
        COALESCE(ARRAY_LENGTH(STRING_TO_ARRAY(opening_crawl, ' '), 1), 0) AS opening_crawl_word_count,
        
        -- Include ETL tracking fields
        url,
        
        -- ETL timestamps
        NULL::TIMESTAMP AS fetch_timestamp,
        NULL::TIMESTAMP AS processed_timestamp,
        created_at AS created,
        updated_at AS edited
    FROM "nerd_facts"."public"."stg_swapi_films"
),

-- Box office and critical reception (manually added since not in SWAPI)
film_performance AS (
    SELECT
        film_id,
        CASE
            WHEN episode_id = 1 THEN 1027.0  -- The Phantom Menace
            WHEN episode_id = 2 THEN 649.4   -- Attack of the Clones
            WHEN episode_id = 3 THEN 850.0   -- Revenge of the Sith
            WHEN episode_id = 4 THEN 775.4   -- A New Hope (adjusted)
            WHEN episode_id = 5 THEN 547.9   -- The Empire Strikes Back (adjusted)
            WHEN episode_id = 6 THEN 475.1   -- Return of the Jedi (adjusted)
            WHEN episode_id = 7 THEN 2068.0  -- The Force Awakens
            WHEN episode_id = 8 THEN 1333.0  -- The Last Jedi
            WHEN episode_id = 9 THEN 1077.0  -- The Rise of Skywalker
            ELSE NULL
        END AS worldwide_box_office_millions,
        
        CASE
            WHEN episode_id = 1 THEN 51  -- The Phantom Menace
            WHEN episode_id = 2 THEN 65  -- Attack of the Clones
            WHEN episode_id = 3 THEN 80  -- Revenge of the Sith
            WHEN episode_id = 4 THEN 93  -- A New Hope
            WHEN episode_id = 5 THEN 94  -- The Empire Strikes Back
            WHEN episode_id = 6 THEN 83  -- Return of the Jedi
            WHEN episode_id = 7 THEN 93  -- The Force Awakens
            WHEN episode_id = 8 THEN 91  -- The Last Jedi
            WHEN episode_id = 9 THEN 52  -- The Rise of Skywalker
            ELSE NULL
        END AS rotten_tomatoes_score
    FROM films
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(f.film_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS film_key,
    
    -- Core identifiers
    f.film_id,
    f.title,
    f.episode_id,
    
    -- Release information
    f.release_date,
    EXTRACT(YEAR FROM f.release_date) AS release_year,
    EXTRACT(DECADE FROM f.release_date) AS release_decade,
    
    -- Film details
    f.director,
    f.producer,
    
    -- Opening crawl with metrics
    f.opening_crawl,
    SUBSTRING(f.opening_crawl FROM 1 FOR 100) || '...' AS opening_crawl_preview,
    f.opening_crawl_word_count,
    
    -- Runtime now comes from the first CTE
    f.runtime_minutes,
    
    -- Film saga classification
    f.trilogy,
    
    -- Timeline ordering
    f.chronological_order AS timeline_order,
    
    -- Film era in timeline with expanded detail
    CASE
        WHEN f.episode_id = 1 THEN 'Republic Era - Trade Federation Crisis'
        WHEN f.episode_id = 2 THEN 'Republic Era - Clone Wars Beginning'
        WHEN f.episode_id = 3 THEN 'Republic Era - Rise of the Empire'
        WHEN f.episode_id = 4 THEN 'Imperial Era - Rebellion Rising'
        WHEN f.episode_id = 5 THEN 'Imperial Era - Rebellion on the Run'
        WHEN f.episode_id = 6 THEN 'Imperial Era - Fall of the Empire'
        WHEN f.episode_id = 7 THEN 'New Republic Era - First Order Emergence'
        WHEN f.episode_id = 8 THEN 'New Republic Era - Resistance Survival'
        WHEN f.episode_id = 9 THEN 'New Republic Era - Final Order Conflict'
        ELSE 'Unknown Era'
    END AS detailed_timeline_era,
    
    -- Broader timeline era
    CASE
        WHEN f.episode_id BETWEEN 1 AND 3 THEN 'Republic Era'
        WHEN f.episode_id BETWEEN 4 AND 6 THEN 'Imperial Era'
        WHEN f.episode_id BETWEEN 7 AND 9 THEN 'New Republic Era'
        ELSE 'Various'
    END AS timeline_era,
    
    -- Production studio
    CASE
        WHEN f.episode_id BETWEEN 1 AND 6 THEN 'Lucasfilm'
        WHEN f.episode_id BETWEEN 7 AND 9 THEN 'Lucasfilm/Disney'
        ELSE 'Other'
    END AS production_studio,
    
    -- Entity counts
    f.character_count,
    f.planet_count,
    f.starship_count,
    f.vehicle_count,
    f.species_count,
    
    -- Entity density metrics (derived)
    ROUND(f.character_count::NUMERIC / NULLIF(f.runtime_minutes, 0), 2) AS characters_per_minute,
    ROUND(f.planet_count::NUMERIC / NULLIF(f.runtime_minutes, 0), 2) AS planets_per_minute,
    
    -- Primary antagonist
    CASE
        WHEN f.episode_id = 1 THEN 'Darth Sidious/Trade Federation'
        WHEN f.episode_id = 2 THEN 'Count Dooku/Separatists'
        WHEN f.episode_id = 3 THEN 'Darth Sidious/General Grievous'
        WHEN f.episode_id = 4 THEN 'Darth Vader/Grand Moff Tarkin'
        WHEN f.episode_id = 5 THEN 'Darth Vader/Emperor Palpatine'
        WHEN f.episode_id = 6 THEN 'Emperor Palpatine/Darth Vader'
        WHEN f.episode_id = 7 THEN 'Kylo Ren/First Order'
        WHEN f.episode_id = 8 THEN 'Kylo Ren/Supreme Leader Snoke'
        WHEN f.episode_id = 9 THEN 'Emperor Palpatine/First Order'
        ELSE 'Unknown'
    END AS primary_antagonist,
    
    -- Primary protagonist
    CASE
        WHEN f.episode_id = 1 THEN 'Qui-Gon Jinn/Obi-Wan Kenobi'
        WHEN f.episode_id = 2 THEN 'Anakin Skywalker/Obi-Wan Kenobi'
        WHEN f.episode_id = 3 THEN 'Anakin Skywalker/Obi-Wan Kenobi'
        WHEN f.episode_id = 4 THEN 'Luke Skywalker'
        WHEN f.episode_id = 5 THEN 'Luke Skywalker'
        WHEN f.episode_id = 6 THEN 'Luke Skywalker'
        WHEN f.episode_id = 7 THEN 'Rey/Finn'
        WHEN f.episode_id = 8 THEN 'Rey'
        WHEN f.episode_id = 9 THEN 'Rey'
        ELSE 'Unknown'
    END AS primary_protagonist,
    
    -- Primary location
    CASE
        WHEN f.episode_id = 1 THEN 'Naboo/Tatooine/Coruscant'
        WHEN f.episode_id = 2 THEN 'Coruscant/Kamino/Geonosis'
        WHEN f.episode_id = 3 THEN 'Coruscant/Mustafar/Kashyyyk'
        WHEN f.episode_id = 4 THEN 'Tatooine/Death Star/Yavin IV'
        WHEN f.episode_id = 5 THEN 'Hoth/Dagobah/Cloud City'
        WHEN f.episode_id = 6 THEN 'Tatooine/Death Star II/Endor'
        WHEN f.episode_id = 7 THEN 'Jakku/Takodana/Starkiller Base'
        WHEN f.episode_id = 8 THEN 'Ahch-To/Canto Bight/Crait'
        WHEN f.episode_id = 9 THEN 'Exegol/Pasaana/Kijimi'
        ELSE 'Unknown'
    END AS primary_locations,
    
    -- Film performance metrics
    fp.worldwide_box_office_millions,
    fp.rotten_tomatoes_score,
    
    -- Critical reception classification
    CASE
        WHEN fp.rotten_tomatoes_score >= 90 THEN 'Critically Acclaimed'
        WHEN fp.rotten_tomatoes_score >= 75 THEN 'Well Received'
        WHEN fp.rotten_tomatoes_score >= 60 THEN 'Mixed to Positive'
        WHEN fp.rotten_tomatoes_score >= 40 THEN 'Mixed'
        ELSE 'Critically Panned'
    END AS critical_reception,
    
    -- Commercial success classification
    CASE
        WHEN fp.worldwide_box_office_millions >= 1500 THEN 'Blockbuster'
        WHEN fp.worldwide_box_office_millions >= 1000 THEN 'Very Successful'
        WHEN fp.worldwide_box_office_millions >= 750 THEN 'Successful'
        WHEN fp.worldwide_box_office_millions >= 500 THEN 'Profitable'
        WHEN fp.worldwide_box_office_millions >= 0 THEN 'Modest'
        ELSE 'Unknown'
    END AS commercial_success,
    
    -- Film complexity metrics - derived from entity counts and crawl
    CASE
        WHEN f.character_count > 30 AND f.planet_count > 5 AND f.opening_crawl_word_count > 100 THEN 'High Complexity'
        WHEN f.character_count > 20 AND f.planet_count > 3 THEN 'Moderate Complexity'
        ELSE 'Standard Complexity'
    END AS narrative_complexity,
    
    -- Major themes
    CASE
        WHEN f.episode_id = 1 THEN 'Trade disputes, Chosen one prophecy'
        WHEN f.episode_id = 2 THEN 'Political manipulation, Clone army'
        WHEN f.episode_id = 3 THEN 'Corruption, Fall to dark side, Order 66'
        WHEN f.episode_id = 4 THEN 'Rebellion, Force awakening, Death Star'
        WHEN f.episode_id = 5 THEN 'Jedi training, Family revelation, Rebellion setback'
        WHEN f.episode_id = 6 THEN 'Redemption, Empire defeat, Jedi return'
        WHEN f.episode_id = 7 THEN 'New heroes, Legacy, Force awakening'
        WHEN f.episode_id = 8 THEN 'Legacy, Letting go of the past, Failure'
        WHEN f.episode_id = 9 THEN 'Identity, Lineage, Final confrontation'
        ELSE 'Various themes'
    END AS major_themes,
    
    -- Source data metadata
    f.url AS source_url,
    f.created AS source_created_at,
    f.edited AS source_edited_at,
    f.fetch_timestamp,
    f.processed_timestamp,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM films f
LEFT JOIN film_performance fp ON f.film_id = fp.film_id
ORDER BY f.chronological_order
  );
  