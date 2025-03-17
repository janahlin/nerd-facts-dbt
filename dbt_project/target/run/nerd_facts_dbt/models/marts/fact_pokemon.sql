
  
    

  create  table "nerd_facts"."public"."fact_pokemon__dbt_tmp"
  
  
    as
  
  (
    WITH pokemon AS (
    SELECT * FROM "nerd_facts"."public"."stg_pokeapi"
)
SELECT
    id,
    pokemon_name,
    height_m,
    weight_kg,
    num_abilities,
    primary_type
FROM pokemon
  );
  