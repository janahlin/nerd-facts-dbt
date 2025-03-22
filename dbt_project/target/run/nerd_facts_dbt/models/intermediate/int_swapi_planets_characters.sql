
  
    

  create  table "nerd_facts"."public"."int_swapi_planets_characters__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: int_swapi_planets_characters
  Description: Creates a relationship table between planets and characters
  Source: Using people.homeworld references
*/

-- Characters with homeworlds (direct homeworld reference)
with character_homeworlds as (
    select
        people_id,
        homeworld::integer as planet_id,
        'Homeworld' as relationship_type
    from "nerd_facts"."public"."stg_swapi_people"
    where homeworld is not null
)

-- Final output with useful metadata
select 
    ch.people_id,
    ch.planet_id,
    ch.relationship_type,
    p.name as character_name,
    pl.name as planet_name,
    p.gender,
    pl.climate,
    pl.terrain,
    -- Create a unique key for the relationship
    md5(cast(coalesce(cast(ch.people_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ch.planet_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as character_planet_key
from 
    character_homeworlds ch
join 
    "nerd_facts"."public"."stg_swapi_people" p on ch.people_id = p.people_id
join 
    "nerd_facts"."public"."stg_swapi_planets" pl on ch.planet_id = pl.planet_id
order by
    pl.name, p.name
  );
  