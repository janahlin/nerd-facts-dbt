
  create view "nerd_facts"."public"."int_films_species__dbt_tmp"
    
    
  as (
    

/*
  Model: int_films_species
  Description: Creates a minimal many-to-many relationship table between films and species
  Source: stg_swapi_films.species JSONB array
*/

with films_with_species as (
    select
        film_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(species::jsonb) as species_id_text
    from "nerd_facts"."public"."stg_swapi_films"
    where species is not null and jsonb_array_length(species::jsonb) > 0
)

select
    f.film_id,
    -- Cast the species ID from text to integer
    f.species_id_text::integer as species_id
from 
    films_with_species f
order by
    film_id, species_id
  );