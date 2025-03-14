
  create view "nerd_facts"."public_public"."stg_swapi_planets__dbt_tmp"
    
    
  as (
    

SELECT
    id,
    name,
    climate,
    terrain,
    diameter::INTEGER AS diameter,
    population::BIGINT AS population,
    gravity,
    orbital_period::INTEGER AS orbital_period,
    rotation_period::INTEGER AS rotation_period,
    surface_water::INTEGER AS surface_water,
    url
FROM "nerd_facts"."raw"."swapi_planets"
  );