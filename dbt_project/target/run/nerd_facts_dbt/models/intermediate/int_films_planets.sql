
  create view "nerd_facts"."public"."int_films_planets__dbt_tmp"
    
    
  as (
    

/*
  Model: int_films_planets
  Description: Creates a minimal many-to-many relationship table between films and planets
  Source: stg_swapi_films.planets JSONB array
*/

with films_with_planets as (
    select
        film_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(planets::jsonb) as planet_id_text
    from "nerd_facts"."public"."stg_swapi_films"
    where planets is not null and jsonb_array_length(planets::jsonb) > 0
)

select
    f.film_id,
    -- Cast the planet ID from text to integer
    f.planet_id_text::integer as planet_id
from 
    films_with_planets f
order by
    film_id, planet_id
  );