select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select title
from "nerd_facts"."public"."stg_swapi_films"
where title is null



      
    ) dbt_internal_test