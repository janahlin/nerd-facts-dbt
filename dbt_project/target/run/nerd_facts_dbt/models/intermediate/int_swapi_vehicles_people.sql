
  create view "nerd_facts"."public"."int_swapi_vehicles_people__dbt_tmp"
    
    
  as (
    

/*
  Model: int_swapi_vehicles_people
  Description: Creates a minimal many-to-many relationship table between vehicles and people (pilots)
  Source: stg_swapi_films.vehicles JSONB array
*/

with vehicles_with_pilots as (
    select
        vehicle_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(pilots::jsonb) as people_id_text
    from "nerd_facts"."public"."stg_swapi_vehicles"
    where pilots is not null and jsonb_array_length(pilots::jsonb) > 0
)

select
    v.vehicle_id,
    -- Cast the starship ID from text to integer
    v.people_id_text::integer as people_id
from 
    vehicles_with_pilots v
order by
    vehicle_id, people_id
  );