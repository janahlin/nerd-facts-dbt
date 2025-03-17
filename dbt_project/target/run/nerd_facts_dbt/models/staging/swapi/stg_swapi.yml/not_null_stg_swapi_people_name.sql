select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select name
from "nerd_facts"."public"."stg_swapi_people"
where name is null



      
    ) dbt_internal_test