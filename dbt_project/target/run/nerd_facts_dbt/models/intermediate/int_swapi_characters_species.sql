
  
    

  create  table "nerd_facts"."public"."int_swapi_characters_species__dbt_tmp"
  
  
    as
  
  (
    

/*
  Model: int_swapi_characters_species
  Description: Creates a comprehensive relationship table between characters and species
  Source: Combines character-to-species and species-to-people relationships
*/

-- Characters with species (direct affiliation)
with character_species as (
    select
        people_id as character_id,  -- This is good, mapping people_id to character_id alias
        jsonb_array_elements_text(species::jsonb)::integer as species_id
    from "nerd_facts"."public"."stg_swapi_people"
    where species is not null and jsonb_array_length(species::jsonb) > 0
),

-- Species with characters (people of that species)
species_characters as (
    select
        species_id,
        jsonb_array_elements_text(people::jsonb)::integer as character_id
    from "nerd_facts"."public"."stg_swapi_species"
    where people is not null and jsonb_array_length(people::jsonb) > 0
),

-- Combine both sources with UNION
combined_relationships as (
    select character_id, species_id from character_species
    union
    select character_id, species_id from species_characters
),

-- Remove any duplicates
unique_relationships as (
    select distinct character_id, species_id 
    from combined_relationships
)

-- Final output with useful metadata
select 
    ur.character_id,
    ur.species_id,
    p.name as character_name,
    s.name as species_name,
    p.gender,
    s.classification,
    s.language,
    -- Create a unique key for the relationship
    md5(cast(coalesce(cast(ur.character_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ur.species_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as character_species_key
from 
    unique_relationships ur
join 
    "nerd_facts"."public"."stg_swapi_people" p on ur.character_id = p.people_id  -- Changed to people_id
join 
    "nerd_facts"."public"."stg_swapi_species" s on ur.species_id = s.species_id
order by
    s.name, p.name
  );
  