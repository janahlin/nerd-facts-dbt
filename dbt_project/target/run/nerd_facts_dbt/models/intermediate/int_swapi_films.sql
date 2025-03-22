
  create view "nerd_facts"."public"."int_swapi_films__dbt_tmp"
    
    
  as (
    

with films as (
    select
        film_id,
        episode_id,
        title,        
        opening_crawl,
        director,
        producer,

        -- Entity counts with error handling and type casting to JSONB
        COALESCE(jsonb_array_length(characters::jsonb), 0) AS character_count,
        COALESCE(jsonb_array_length(planets::jsonb), 0) AS planet_count,
        COALESCE(jsonb_array_length(starships::jsonb), 0) AS starship_count,
        COALESCE(jsonb_array_length(vehicles::jsonb), 0) AS vehicle_count,
        COALESCE(jsonb_array_length(species::jsonb), 0) AS species_count, 

        -- Derived film classification (use the casted episode_id field)
        CASE
        WHEN CAST(episode_id AS INTEGER) BETWEEN 1 AND 3 THEN 'Prequel Trilogy'
        WHEN CAST(episode_id AS INTEGER) BETWEEN 4 AND 6 THEN 'Original Trilogy'
        WHEN CAST(episode_id AS INTEGER) BETWEEN 7 AND 9 THEN 'Sequel Trilogy'
        ELSE 'Anthology'
        END AS trilogy,       

        -- Era classification (use the casted episode_id field)
        CASE
            WHEN CAST(episode_id AS INTEGER) BETWEEN 1 AND 3 THEN 'Republic Era'
            WHEN CAST(episode_id AS INTEGER) BETWEEN 4 AND 6 THEN 'Imperial Era'
            WHEN CAST(episode_id AS INTEGER) BETWEEN 7 AND 9 THEN 'New Republic Era'
            ELSE 'Various'
        END AS era,

        release_date,
        created_at,
        edited_at,
        dbt_loaded_at,
        url
    from "nerd_facts"."public"."stg_swapi_films"
)

select * from films
  );