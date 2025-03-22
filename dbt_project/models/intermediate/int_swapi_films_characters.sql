{{
  config(
    materialized = 'table'
  )
}}

/*
  Model: int_swapi_films_characters
  Description: Creates a relationship table between films and characters
  Source: Using films.characters array
*/

-- Get relationships from films perspective (the only available direction)
with films_to_characters as (
    select
        film_id,
        jsonb_array_elements_text(characters::jsonb)::integer as people_id
    from {{ ref('stg_swapi_films') }}
    where characters is not null and jsonb_array_length(characters::jsonb) > 0
)

-- Final output with useful metadata
select 
    fc.film_id,
    fc.people_id,
    f.title as film_title,
    p.name as character_name,
    f.release_date,
    p.gender,
    p.birth_year,
    -- Create a unique key for the relationship
    {{ dbt_utils.generate_surrogate_key(['fc.film_id', 'fc.people_id']) }} as film_character_key
from 
    films_to_characters fc
join 
    {{ ref('stg_swapi_films') }} f on fc.film_id = f.film_id
join 
    {{ ref('stg_swapi_people') }} p on fc.people_id = p.people_id
order by
    f.release_date, p.name
