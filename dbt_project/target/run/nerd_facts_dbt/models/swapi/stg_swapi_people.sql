
  create view "nerd_facts"."public"."stg_swapi_people__dbt_tmp"
    
    
  as (
    

SELECT
    id,
    name,
    height::DECIMAL AS height,
    mass::DECIMAL AS mass,
    hair_color,
    skin_color,
    eye_color,
    birth_year,
    gender,
    homeworld::INTEGER AS homeworld_id, -- Convert homeworld URL to integer UID    
    url
FROM "nerd_facts"."raw"."swapi_people"
  );