
  create view "nerd_facts"."public"."dim_people__dbt_tmp"
    
    
  as (
    

SELECT
    id,
    name,
    -- ✅ Convert `height` safely
    CASE 
        WHEN height ~ '^\d+$' THEN height::DECIMAL
        ELSE NULL
    END AS height,
    -- ✅ Convert `mass` safely
    CASE 
        WHEN mass ~ '^\d+$' THEN mass::DECIMAL
        ELSE NULL
    END AS mass,
    hair_color,
    skin_color,
    eye_color,
    birth_year,
    gender,
    homeworld::INTEGER AS homeworld_id,
    url
FROM "nerd_facts"."raw"."swapi_people"
  );