
  
    

  create  table "nerd_facts"."public"."fact_starships__dbt_tmp"
  
  
    as
  
  (
    WITH starships AS (
    SELECT * FROM "nerd_facts"."public"."stg_swapi"
)
SELECT
    id,
    starship_name,
    model,
    manufacturer,
    max_speed,
    crew,
    passengers
FROM starships
  );
  