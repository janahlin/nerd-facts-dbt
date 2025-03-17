select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id
from "nerd_facts"."public"."stg_swapi_species"
where id is null



      
    ) dbt_internal_test