select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select starship_name
from "nerd_facts"."public"."stg_swapi"
where starship_name is null



      
    ) dbt_internal_test