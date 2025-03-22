

/*
  Model: int_films_vehicles
  Description: Creates a minimal many-to-many relationship table between films and vehicles
  Source: stg_swapi_films.vehicles JSONB array
*/

with films_with_vehicles as (
    select
        film_id,
        -- Extract each vehicles ID from the JSONB array
        jsonb_array_elements_text(vehicles::jsonb) as vehicle_id_text
    from "nerd_facts"."public"."stg_swapi_films"
    where vehicles is not null and jsonb_array_length(vehicles::jsonb) > 0
)

select
    f.film_id,
    -- Cast the vehicle ID from text to integer
    f.vehicle_id_text::integer as vehicle_id
from 
    films_with_vehicles f
order by
    film_id, vehicle_id