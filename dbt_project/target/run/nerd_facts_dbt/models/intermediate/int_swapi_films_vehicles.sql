
  
    

  create  table "nerd_facts"."public"."int_swapi_films_vehicles__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: int_swapi_films_vehicles
  Description: Creates a comprehensive many-to-many relationship table between films and vehicles
  Source: Combines data from both films.vehicles and vehicles.films arrays for completeness
*/

-- Get relationships from films perspective
with films_to_vehicles as (
    select
        film_id,
        jsonb_array_elements_text(vehicles::jsonb)::integer as vehicle_id
    from "nerd_facts"."public"."stg_swapi_films"
    where vehicles is not null and jsonb_array_length(vehicles::jsonb) > 0
),

-- Get relationships from vehicles perspective
vehicles_to_films as (
    select
        vehicle_id,
        jsonb_array_elements_text(films::jsonb)::integer as film_id
    from "nerd_facts"."public"."stg_swapi_vehicles"
    where films is not null and jsonb_array_length(films::jsonb) > 0
),

-- Combine both sources with UNION
combined_relationships as (
    select film_id, vehicle_id from films_to_vehicles
    union
    select film_id, vehicle_id from vehicles_to_films
),

-- Remove any duplicates
unique_relationships as (
    select distinct film_id, vehicle_id 
    from combined_relationships
)

-- Final output with useful metadata
select 
    ur.film_id,
    ur.vehicle_id,
    f.title as film_title,
    v.vehicle_name,
    f.release_date,
    v.model,
    v.manufacturer,
    -- Create a unique key for the relationship
    md5(cast(coalesce(cast(ur.film_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ur.vehicle_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as film_vehicle_key
from 
    unique_relationships ur
join 
    "nerd_facts"."public"."stg_swapi_films" f on ur.film_id = f.film_id
join 
    "nerd_facts"."public"."stg_swapi_vehicles" v on ur.vehicle_id = v.vehicle_id
order by
    f.release_date, v.vehicle_name
  );
  