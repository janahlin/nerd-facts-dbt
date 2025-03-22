
  create view "nerd_facts"."public"."int_films_characters__dbt_tmp"
    
    
  as (
    

/*
  Model: int_films_characters
  Description: Creates a minimal many-to-many relationship table between films and characters
  Source: stg_swapi_films.characters JSONB array
*/

with films_with_characters as (
    select
        film_id,
        -- Extract each vehicles ID from the JSONB array
        jsonb_array_elements_text(characters::jsonb) as character_id_text
    from "nerd_facts"."public"."stg_swapi_films"
    where characters is not null and jsonb_array_length(characters::jsonb) > 0
)

select
    f.film_id,
    -- Cast the character ID from text to integer
    f.character_id_text::integer as character_id
from 
    films_with_characters f
order by
    film_id, character_id
  );