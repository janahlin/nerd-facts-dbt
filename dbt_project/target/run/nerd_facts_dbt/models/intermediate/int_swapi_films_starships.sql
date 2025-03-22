
  
    

  create  table "nerd_facts"."public"."int_swapi_films_starships__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: int_swapi_films_starships
  Description: Creates a comprehensive many-to-many relationship table between films and starships
  Source: Combines data from both films.starships and starships.films arrays for completeness
*/

-- Get relationships from films perspective
with films_to_starships as (
    select
        film_id,
        jsonb_array_elements_text(starships::jsonb)::integer as starship_id
    from "nerd_facts"."public"."stg_swapi_films"
    where starships is not null and jsonb_array_length(starships::jsonb) > 0
),

-- Get relationships from starships perspective
starships_to_films as (
    select
        starship_id,
        jsonb_array_elements_text(films::jsonb)::integer as film_id
    from "nerd_facts"."public"."stg_swapi_starships"
    where films is not null and jsonb_array_length(films::jsonb) > 0
),

-- Combine both sources with UNION
combined_relationships as (
    select film_id, starship_id from films_to_starships
    union
    select film_id, starship_id from starships_to_films
),

-- Remove any duplicates
unique_relationships as (
    select distinct film_id, starship_id 
    from combined_relationships
)

-- Final output with useful metadata
select 
    ur.film_id,
    ur.starship_id,
    f.title as film_title,
    s.starship_name,
    f.release_date,
    -- Create a unique key for the relationship
    md5(cast(coalesce(cast(ur.film_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ur.starship_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as film_starship_key
from 
    unique_relationships ur
join 
    "nerd_facts"."public"."stg_swapi_films" f on ur.film_id = f.film_id
join 
    "nerd_facts"."public"."stg_swapi_starships" s on ur.starship_id = s.starship_id
order by
    f.release_date, s.starship_name
  );
  