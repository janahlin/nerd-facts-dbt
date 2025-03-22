{{
  config(
    materialized = 'view'
  )
}}

/*
  Model: int_swapi_starships_people
  Description: Creates a minimal many-to-many relationship table between films and people (pilots)
  Source: stg_swapi_films.starships JSONB array
*/

with starships_with_pilots as (
    select
        starship_id,
        -- Extract each planet ID from the JSONB array
        jsonb_array_elements_text(pilots::jsonb) as people_id_text
    from {{ ref('stg_swapi_starships') }}
    where pilots is not null and jsonb_array_length(pilots::jsonb) > 0
)

select
    s.starship_id,
    -- Cast the starship ID from text to integer
    s.people_id_text::integer as people_id
from 
    starships_with_pilots s
order by
    starship_id, people_id
