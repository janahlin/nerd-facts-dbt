{{
  config(
    materialized = 'table'
  )
}}

/*
  Model: int_swapi_films_species
  Description: Creates a relationship table between films and species
  Source: Using films.species array
*/

-- Get relationships from films perspective (only direction available)
with films_to_species as (
    select
        film_id,
        jsonb_array_elements_text(species::jsonb)::integer as species_id
    from {{ ref('stg_swapi_films') }}
    where species is not null and jsonb_array_length(species::jsonb) > 0
)

-- Final output with useful metadata
select 
    fs.film_id,
    fs.species_id,
    f.title as film_title,
    -- Using different field names based on your stg_swapi_species structure
    s.species_name, -- Changed from s.name
    f.release_date,
    s.classification,
    s.language,
    -- Create a unique key for the relationship
    {{ dbt_utils.generate_surrogate_key(['fs.film_id', 'fs.species_id']) }} as film_species_key
from 
    films_to_species fs
join 
    {{ ref('stg_swapi_films') }} f on fs.film_id = f.film_id
join 
    {{ ref('stg_swapi_species') }} s on fs.species_id = s.species_id
order by
    f.release_date, s.species_name -- Changed sort order to use species_name
