
  create view "nerd_facts"."public"."int_swapi_people__dbt_tmp"
    
    
  as (
    

with people as (
    select        
        people_id,        
        name,        
        LOWER(COALESCE(hair_color, 'unknown')) AS hair_color,
        LOWER(COALESCE(skin_color, 'unknown')) AS skin_color,
        LOWER(COALESCE(eye_color, 'unknown')) AS eye_color,
        birth_year,
        LOWER(COALESCE(gender, 'unknown')) AS gender,
        homeworld as homeworld_id,                
        height,
        mass,
        -- Force detection
        CASE
        WHEN LOWER(name) IN ('luke skywalker', 'darth vader', 'obi-wan kenobi', 'yoda', 
                           'emperor palpatine', 'count dooku', 'qui-gon jinn', 'mace windu',
                           'rey', 'kylo ren', 'anakin skywalker', 'leia organa', 
                           'ahsoka tano', 'darth maul')
        THEN TRUE
        ELSE FALSE
        END AS force_sensitive,
        created_at,
        edited_at,  
        dbt_loaded_at,
        url
    from "nerd_facts"."public"."stg_swapi_people"
)

select * from people
  );