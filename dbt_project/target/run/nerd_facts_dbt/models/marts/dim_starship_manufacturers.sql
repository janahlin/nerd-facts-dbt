
  
    

  create  table "nerd_facts"."public"."dim_starship_manufacturers__dbt_tmp"
  
  
    as
  
  (
    WITH manufacturers AS (
    SELECT DISTINCT
        manufacturer,
        COUNT(*) AS num_starships
    FROM "nerd_facts"."public"."stg_swapi"
    GROUP BY manufacturer
)
SELECT * FROM manufacturers
  );
  