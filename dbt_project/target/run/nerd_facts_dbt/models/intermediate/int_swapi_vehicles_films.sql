
  create view "nerd_facts"."public"."int_swapi_vehicles_films__dbt_tmp"
    
    
  as (
    

/*
  Model: int_swapi_vehicles_films
  Description: Creates a minimal many-to-many relationship table between films and vehicles
  Source: stg_swapi_films.vehicles JSONB array
*/

with vehicles_with_films as (
    select
        vehicle_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(films::jsonb) as film_id_text
    from "nerd_facts"."public"."stg_swapi_vehicles"
    where films is not null and jsonb_array_length(films::jsonb) > 0
)

select
    v.vehicle_id,
    -- Cast the starship ID from text to integer
    v.film_id_text::integer as film_id
from 
    vehicles_with_films v
order by
    vehicle_id, film_id
  );