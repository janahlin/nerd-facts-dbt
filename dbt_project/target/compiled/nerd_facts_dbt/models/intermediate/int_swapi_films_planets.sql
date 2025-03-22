

/*
  Model: int_swapi_films_planets
  Description: Creates a relationship table between films and planets
  Source: Using films.planets array
*/

-- Get relationships from films perspective (only direction available)
with films_to_planets as (
    select
        film_id,
        jsonb_array_elements_text(planets::jsonb)::integer as planet_id
    from "nerd_facts"."public"."stg_swapi_films"
    where planets is not null and jsonb_array_length(planets::jsonb) > 0
)

-- Final output with useful metadata
select 
    fp.film_id,
    fp.planet_id,
    f.title as film_title,
    p.name as planet_name,
    f.release_date,
    p.climate,
    p.terrain,
    -- Create a unique key for the relationship
    md5(cast(coalesce(cast(fp.film_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fp.planet_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as film_planet_key
from 
    films_to_planets fp
join 
    "nerd_facts"."public"."stg_swapi_films" f on fp.film_id = f.film_id
join 
    "nerd_facts"."public"."stg_swapi_planets" p on fp.planet_id = p.planet_id
order by
    f.release_date, p.name