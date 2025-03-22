
  create view "nerd_facts"."public"."int_swapi_species__dbt_tmp"
    
    
  as (
    

with species as (        
    select
    species_id,
    species_name,
    classification,
    designation,        
    skin_colors,
    hair_colors,
    eye_colors,                
    language,    
    average_lifespan,
    average_height,
    people,
    homeworld,
    created_at,
    edited_at,
    dbt_loaded_at,
    url
    from "nerd_facts"."public"."stg_swapi_species"
)

select * from species
  );