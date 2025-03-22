

/*
  Model: int_swapi_starships_films
  Description: Creates a minimal many-to-many relationship table between films and starships
  Source: stg_swapi_films.starships JSONB array
*/

with starships_with_films as (
    select
        starship_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(films::jsonb) as film_id_text
    from "nerd_facts"."public"."stg_swapi_starships"
    where films is not null and jsonb_array_length(films::jsonb) > 0
)

select
    s.starship_id,
    -- Cast the starship ID from text to integer
    s.film_id_text::integer as film_id
from 
    starships_with_films s
order by
    starship_id, film_id