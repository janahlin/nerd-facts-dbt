

/*
  Model: int_films_starships
  Description: Creates a minimal many-to-many relationship table between films and starships
  Source: stg_swapi_films.starships JSONB array
*/

with films_with_starships as (
    select
        film_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(starships::jsonb) as starship_id_text
    from "nerd_facts"."public"."stg_swapi_films"
    where starships is not null and jsonb_array_length(starships::jsonb) > 0
)

select
    f.film_id,
    -- Cast the starship ID from text to integer
    f.starship_id_text::integer as starship_id
from 
    films_with_starships f
order by
    film_id, starship_id